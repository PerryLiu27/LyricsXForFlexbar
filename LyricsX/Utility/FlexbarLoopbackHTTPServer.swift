import AppKit
import Darwin
import Foundation
import LyricsXFoundation
import MusicPlayer
import OpenCC
import SwiftCF

/// Loopback-only HTTP server exposing playback + lyrics as JSON for Flexbar (`GET /state`).
final class FlexbarLoopbackHTTPServer {
    static let shared = FlexbarLoopbackHTTPServer()

    private let queue = DispatchQueue(label: "LyricsXForFlexbar.FlexbarHTTP", qos: .utility)
    /// Accept loop blocks until `listenFD` closes; keep it off `queue` so `stop` / `restart` can run.
    private let acceptQueue = DispatchQueue(label: "LyricsXForFlexbar.FlexbarHTTP.accept", qos: .utility)
    private var listenFD: Int32 = -1
    private var acceptTask: DispatchWorkItem?
    /// Bumped on each stop so the accept loop exits without racing `fd` reuse.
    private var listenSession = 0
    private let stateCacheLock = NSLock()
    private let spotifyArtworkLock = NSLock()
    private var cachedCompactKey: String?
    private var cachedCompactData: Data?
    private var cachedFullKey: String?
    private var cachedFullData: Data?
    private var stateRequestCount = 0
    private var stateCacheHitCount = 0
    private var spotifyArtworkPendingTrackIDs = Set<String>()

    private struct StateSnapshot {
        var cacheKey: String
        var payload: [String: Any]
        var trackIDForArtwork: String?
        var spotifyArtworkURL: String?
    }

    private struct LyricsWindowSlice {
        var lines: [[String: Any]]
        var lineWindowStartIndex: Int
        var currentLineStart: TimeInterval?
        var nextLineText: String
    }

    private struct TemplateSnapshot {
        var key: String
        var track: [String: Any]
        var lyrics: [String: Any]
        var trackIDForArtwork: String?
        var spotifyArtworkURL: String?
    }

    private init() {}

    func updateFromDefaults() {
        queue.async { [weak self] in
            self?.restartIfNeededLocked()
        }
    }

    func stop() {
        queue.async { [weak self] in
            self?.stopLocked()
        }
    }

    private func restartIfNeededLocked() {
        stopLocked()
        guard defaults[.flexbarHTTPServerEnabled] else {
            return
        }
        let session = listenSession
        let p = defaults[.flexbarHTTPServerPort]
        guard p > 0, p <= Int(UInt16.max) else {
            log("Flexbar HTTP: invalid port \(p)")
            return
        }
        let port = UInt16(p)
        let fd = socket(AF_INET, SOCK_STREAM, 0)
        guard fd >= 0 else {
            log("Flexbar HTTP: socket failed")
            return
        }
        var yes: Int32 = 1
        setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &yes, socklen_t(MemoryLayout.size(ofValue: yes)))

        var addr = sockaddr_in()
        addr.sin_len = UInt8(MemoryLayout.size(ofValue: addr))
        addr.sin_family = sa_family_t(AF_INET)
        addr.sin_port = port.bigEndian
        addr.sin_addr.s_addr = inet_addr("127.0.0.1")

        let addrLen = socklen_t(MemoryLayout<sockaddr_in>.size)
        let bindResult = withUnsafePointer(to: &addr) {
            $0.withMemoryRebound(to: sockaddr.self, capacity: 1) {
                bind(fd, $0, addrLen)
            }
        }
        guard bindResult == 0 else {
            log("Flexbar HTTP: bind 127.0.0.1:\(port) failed (\(String(cString: strerror(errno))))")
            close(fd)
            return
        }
        guard listen(fd, 16) == 0 else {
            log("Flexbar HTTP: listen failed")
            close(fd)
            return
        }
        listenFD = fd
        log("Flexbar HTTP: listening on http://127.0.0.1:\(port)/state")

        let work = DispatchWorkItem { [weak self] in
            self?.acceptLoop(fd: fd, session: session)
        }
        acceptTask = work
        acceptQueue.async(execute: work)
    }

    private func stopLocked() {
        acceptTask?.cancel()
        acceptTask = nil
        listenSession &+= 1
        if listenFD >= 0 {
            shutdown(listenFD, SHUT_RDWR)
            close(listenFD)
            listenFD = -1
        }
    }

    private func acceptLoop(fd: Int32, session: Int) {
        while session == listenSession {
            var cliAddr = sockaddr_in()
            var cliLen = socklen_t(MemoryLayout<sockaddr_in>.size)
            let client = withUnsafeMutablePointer(to: &cliAddr) {
                $0.withMemoryRebound(to: sockaddr.self, capacity: 1) {
                    accept(fd, $0, &cliLen)
                }
            }
            guard client >= 0 else {
                if errno == EINTR { continue }
                break
            }
            // Must not schedule on `queue`: acceptLoop runs on that same serial queue and
            // would starve these blocks until accept() stops (never), so curl would hang.
            DispatchQueue.global(qos: .utility).async { [weak self] in
                self?.handleClient(clientFD: client)
            }
        }
    }

    private func handleClient(clientFD: Int32) {
        defer { close(clientFD) }
        var buffer = [UInt8](repeating: 0, count: 8192)
        let n = recv(clientFD, &buffer, buffer.count, 0)
        guard n > 0 else { return }
        let req = String(decoding: buffer.prefix(n), as: UTF8.self)
        guard let line = req.split(separator: "\r\n", omittingEmptySubsequences: false).first else {
            writeTextResponse(fd: clientFD, status: 400, body: "Bad Request")
            return
        }
        let parts = line.split(separator: " ")
        guard parts.count >= 2 else {
            writeTextResponse(fd: clientFD, status: 400, body: "Bad Request")
            return
        }
        let method = String(parts[0])
        let rawPath = String(parts[1])
        let components = URLComponents(string: rawPath)
        let path = components?.path ?? rawPath
        guard method == "GET" else {
            writeTextResponse(fd: clientFD, status: 405, body: "Method Not Allowed")
            return
        }

        switch path {
        case "/state":
            let includeFullLines = components?.queryItems?.contains(where: {
                ($0.name == "full" || $0.name == "fullLines") && $0.value == "1"
            }) == true
            let sortedKeys = components?.queryItems?.contains(where: {
                $0.name == "sorted" && $0.value == "1"
            }) == true
            let data = buildStateJSONData(includeFullLines: includeFullLines, sortedKeys: sortedKeys)
            writeJSONResponse(fd: clientFD, json: data)
        case "/health":
            writeTextResponse(fd: clientFD, status: 200, body: "ok")
        default:
            writeTextResponse(fd: clientFD, status: 404, body: "Not Found")
        }
    }

    private func buildStateJSONData(includeFullLines: Bool, sortedKeys: Bool) -> Data {
        let snapshot = DispatchQueue.main.sync {
            Self.captureStateSnapshotOnMainThread(includeFullLines: includeFullLines)
        }
        if let cached = cachedStateJSONIfValid(cacheKey: snapshot.cacheKey, includeFullLines: includeFullLines) {
            registerStateRequest(cacheHit: true)
            return cached
        }

        var payload = snapshot.payload
        if var td = payload["track"] as? [String: Any],
           td["artworkBase64"] == nil,
           let trackID = snapshot.trackIDForArtwork {
            if let cachedArtwork = Self.artworkJPEGBase64Cache.object(forKey: trackID as NSString) {
                td["artworkBase64"] = cachedArtwork as String
                payload["track"] = td
            } else if let spotifyArtworkURL = snapshot.spotifyArtworkURL {
                queueSpotifyArtworkFetchIfNeeded(artworkURLString: spotifyArtworkURL, trackID: trackID)
            }
        }

        let options: JSONSerialization.WritingOptions = sortedKeys ? [.sortedKeys] : []
        let data = (try? JSONSerialization.data(withJSONObject: payload, options: options)) ?? Data("{}".utf8)
        storeCachedStateJSON(data, cacheKey: snapshot.cacheKey, includeFullLines: includeFullLines)
        registerStateRequest(cacheHit: false)
        return data
    }

    private static var compactTemplateCacheKey: NSString?
    private static var compactTemplateTrack: [String: Any] = [:]
    private static var compactTemplateLyrics: [String: Any] = [:]
    private static var compactTemplateSpotifyArtworkURL: String?
    private static var compactTemplateTrackIDForArtwork: String?
    private static var fullTemplateCacheKey: NSString?
    private static var fullTemplateTrack: [String: Any] = [:]
    private static var fullTemplateLyrics: [String: Any] = [:]
    private static var fullTemplateSpotifyArtworkURL: String?
    private static var fullTemplateTrackIDForArtwork: String?

    private static func captureStateSnapshotOnMainThread(includeFullLines: Bool) -> StateSnapshot {
        // Do not call `updatePlayerState()` here — for Spotify it re-applies broken SB `playerPosition` and
        // clobbers progress derived from notifications (see Selected.scheduleManualUpdate).
        let playback = selectedPlayer.playbackState
        let playerName = String(describing: selectedPlayer.name)
        let position = selectedPlayer.playbackTime
        let track = selectedPlayer.currentTrack
        let lyrics = AppController.shared.currentLyrics
        let lineIndex = AppController.shared.currentLineIndex
        let offsetMs = AppController.shared.lyricsOffset

        let template = templateSnapshotOnMainThread(
            includeFullLines: includeFullLines,
            track: track,
            lyrics: lyrics,
            lineIndex: lineIndex,
            offsetMs: offsetMs
        )

        let posBucket = Int((max(0, position) * 5.0).rounded(.down))
        let cacheKey = "\(template.key)|\(playback.isPlaying ? 1 : 0)|\(posBucket)"
        let payload: [String: Any] = [
            "schemaVersion": 3,
            "app": "LyricsXForFlexbar",
            "playing": playback.isPlaying,
            // Use `playbackTime` so position matches the player’s live SB clock (Spotify / Apple Music).
            // `playbackState.time` can lag the same value in edge cases; Flexbar needs smooth scrubbing.
            "position": position,
            "player": playerName,
            "track": template.track,
            "lyrics": template.lyrics,
        ]
        return StateSnapshot(
            cacheKey: cacheKey,
            payload: payload,
            trackIDForArtwork: template.trackIDForArtwork,
            spotifyArtworkURL: template.spotifyArtworkURL
        )
    }

    private static func templateSnapshotOnMainThread(
        includeFullLines: Bool,
        track: MusicTrack?,
        lyrics: Lyrics?,
        lineIndex: Int?,
        offsetMs: Int
    ) -> TemplateSnapshot {
        let key = templateCacheKey(
            includeFullLines: includeFullLines,
            track: track,
            lyrics: lyrics,
            lineIndex: lineIndex,
            offsetMs: offsetMs
        )

        if includeFullLines {
            let nsKey = key as NSString
            if let cachedKey = fullTemplateCacheKey, cachedKey == nsKey {
                return TemplateSnapshot(
                    key: key,
                    track: fullTemplateTrack,
                    lyrics: fullTemplateLyrics,
                    trackIDForArtwork: fullTemplateTrackIDForArtwork,
                    spotifyArtworkURL: fullTemplateSpotifyArtworkURL
                )
            }
            let rebuilt = rebuildTemplateOnMainThread(
                includeFullLines: true,
                track: track,
                lyrics: lyrics,
                lineIndex: lineIndex,
                offsetMs: offsetMs
            )
            fullTemplateCacheKey = nsKey
            fullTemplateTrack = rebuilt.track
            fullTemplateLyrics = rebuilt.lyrics
            fullTemplateTrackIDForArtwork = rebuilt.trackIDForArtwork
            fullTemplateSpotifyArtworkURL = rebuilt.spotifyArtworkURL
            return rebuilt
        }

        let nsKey = key as NSString
        if let cachedKey = compactTemplateCacheKey, cachedKey == nsKey {
            return TemplateSnapshot(
                key: key,
                track: compactTemplateTrack,
                lyrics: compactTemplateLyrics,
                trackIDForArtwork: compactTemplateTrackIDForArtwork,
                spotifyArtworkURL: compactTemplateSpotifyArtworkURL
            )
        }
        let rebuilt = rebuildTemplateOnMainThread(
            includeFullLines: false,
            track: track,
            lyrics: lyrics,
            lineIndex: lineIndex,
            offsetMs: offsetMs
        )
        compactTemplateCacheKey = nsKey
        compactTemplateTrack = rebuilt.track
        compactTemplateLyrics = rebuilt.lyrics
        compactTemplateTrackIDForArtwork = rebuilt.trackIDForArtwork
        compactTemplateSpotifyArtworkURL = rebuilt.spotifyArtworkURL
        return rebuilt
    }

    private static func rebuildTemplateOnMainThread(
        includeFullLines: Bool,
        track: MusicTrack?,
        lyrics: Lyrics?,
        lineIndex: Int?,
        offsetMs: Int
    ) -> TemplateSnapshot {
        var trackDict: [String: Any] = [:]
        var trackIDForArtwork: String?
        var spotifyArtworkURL: String?

        if let track {
            let tid = String(describing: track.id)
            trackIDForArtwork = tid
            trackDict["id"] = tid
            trackDict["title"] = track.title ?? ""
            trackDict["artist"] = track.artist ?? ""
            trackDict["album"] = track.album ?? ""
            trackDict["duration"] = Self.resolvedDurationSeconds(for: track)
            if let cached = Self.artworkJPEGBase64Cache.object(forKey: tid as NSString) {
                trackDict["artworkBase64"] = cached as String
            } else {
                let artworkImage = Self.nsImageFromScriptingArtworksIfPresent(originalTrack: track.originalTrack) ?? track.artwork
                if let b64 = Self.jpegBase64Artwork(from: artworkImage) {
                    trackDict["artworkBase64"] = b64
                    Self.artworkJPEGBase64Cache.setObject(b64 as NSString, forKey: tid as NSString)
                } else if selectedPlayer.name == .spotify {
                    spotifyArtworkURL = Self.spotifyArtworkURLString(for: track)
                }
            }
        }

        var lyricsDict: [String: Any] = [
            "available": lyrics != nil,
            "offsetMs": offsetMs,
            "currentLineReading": [],
            "currentLineWordTiming": [],
            "currentLineReadingSig": "0",
            "currentLineWordTimingSig": "0",
            "nextLine": "",
            "currentLineStart": NSNull(),
        ]

        if let lyrics {
            lyricsDict["adjustedTimeDelay"] = lyrics.adjustedTimeDelay
            let languageCode = lyrics.metadata.language
            lyricsDict["currentLineIndex"] = lineIndex as Any
            let window = buildLyricsWindowSlice(lyrics: lyrics, currentLineIndex: lineIndex)
            lyricsDict["nextLine"] = window.nextLineText
            lyricsDict["currentLineStart"] = window.currentLineStart as Any
            if includeFullLines {
                lyricsDict["lines"] = lyrics.lines.map { line in
                    ["t": line.position, "text": line.content]
                }
            } else {
                lyricsDict["lines"] = window.lines
                lyricsDict["lineWindowStartIndex"] = window.lineWindowStartIndex
            }
            if let idx = lineIndex, lyrics.lines.indices.contains(idx) {
                var text = lyrics.lines[idx].content
                if let converter = ChineseConverter.shared, lyrics.metadata.language?.hasPrefix("zh") == true {
                    text = converter.convert(text)
                }
                lyricsDict["currentLine"] = text
                let readingRuns = Self.currentLineReading(for: text, languageCode: languageCode)
                let timingTags = Self.currentLineWordTiming(lyrics: lyrics, lineIndex: idx)
                lyricsDict["currentLineReading"] = readingRuns
                lyricsDict["currentLineWordTiming"] = timingTags
                lyricsDict["currentLineReadingSig"] = Self.readingSignature(readingRuns)
                lyricsDict["currentLineWordTimingSig"] = Self.wordTimingSignature(timingTags)
                var translation = ""
                if let code = lyrics.metadata.translationLanguages.first,
                   var trans = lyrics.lines[idx].attachments[.translation(languageCode: code)] {
                    if code.hasPrefix("zh"), let converter = ChineseConverter.shared {
                        trans = converter.convert(trans)
                    }
                    translation = trans
                }
                lyricsDict["currentTranslation"] = translation
            } else {
                lyricsDict["currentLine"] = ""
                lyricsDict["currentTranslation"] = ""
                lyricsDict["currentLineReading"] = []
                lyricsDict["currentLineWordTiming"] = []
                lyricsDict["currentLineReadingSig"] = "0"
                lyricsDict["currentLineWordTimingSig"] = "0"
            }
        } else {
            lyricsDict["lines"] = []
            lyricsDict["currentLineIndex"] = NSNull()
            lyricsDict["currentLine"] = ""
            lyricsDict["currentTranslation"] = ""
            lyricsDict["currentLineReading"] = []
            lyricsDict["currentLineWordTiming"] = []
        }

        return TemplateSnapshot(
            key: templateCacheKey(
                includeFullLines: includeFullLines,
                track: track,
                lyrics: lyrics,
                lineIndex: lineIndex,
                offsetMs: offsetMs
            ),
            track: trackDict,
            lyrics: lyricsDict,
            trackIDForArtwork: trackIDForArtwork,
            spotifyArtworkURL: spotifyArtworkURL
        )
    }

    private static func templateCacheKey(
        includeFullLines: Bool,
        track: MusicTrack?,
        lyrics: Lyrics?,
        lineIndex: Int?,
        offsetMs: Int
    ) -> String {
        let trackID = track.map { String(describing: $0.id) } ?? ""
        let trackTitle = track?.title ?? ""
        let trackArtist = track?.artist ?? ""
        let trackAlbum = track?.album ?? ""
        let hasArtwork = trackID.isEmpty ? 0 : (artworkJPEGBase64Cache.object(forKey: trackID as NSString) != nil ? 1 : 0)
        var currentLineText = ""
        var translationText = ""
        var adjustedTimeDelay: TimeInterval = 0
        var linesCount = 0
        if let lyrics {
            adjustedTimeDelay = lyrics.adjustedTimeDelay
            linesCount = lyrics.lines.count
            if let idx = lineIndex, lyrics.lines.indices.contains(idx) {
                currentLineText = lyrics.lines[idx].content
                if let code = lyrics.metadata.translationLanguages.first,
                   let trans = lyrics.lines[idx].attachments[.translation(languageCode: code)] {
                    translationText = trans
                }
            }
        }
        return [
            includeFullLines ? "full" : "compact",
            trackID,
            trackTitle,
            trackArtist,
            trackAlbum,
            String(hasArtwork),
            String(lineIndex ?? -1),
            String(offsetMs),
            String(adjustedTimeDelay),
            String(linesCount),
            currentLineText,
            translationText,
        ].joined(separator: "|")
    }

    private static func buildLyricsWindowSlice(lyrics: Lyrics, currentLineIndex: Int?) -> LyricsWindowSlice {
        guard let idx = currentLineIndex, lyrics.lines.indices.contains(idx) else {
            return LyricsWindowSlice(lines: [], lineWindowStartIndex: 0, currentLineStart: nil, nextLineText: "")
        }
        let currentLine = lyrics.lines[idx]
        let nextLine = idx + 1 < lyrics.lines.count ? lyrics.lines[idx + 1] : nil
        var lines: [[String: Any]] = [["t": currentLine.position, "text": currentLine.content]]
        if let nextLine {
            lines.append(["t": nextLine.position, "text": nextLine.content])
        }
        return LyricsWindowSlice(
            lines: lines,
            lineWindowStartIndex: idx,
            currentLineStart: currentLine.position,
            nextLineText: nextLine?.content ?? ""
        )
    }

    private static func readingSignature(_ runs: [[String: Any]]) -> String {
        guard !runs.isEmpty else { return "0" }
        let count = runs.count
        let first = runs.first
        let last = runs.last
        let fst = "\(first?["start"] ?? -1)-\(first?["end"] ?? -1)-\(first?["read"] ?? "")"
        let lst = "\(last?["start"] ?? -1)-\(last?["end"] ?? -1)-\(last?["read"] ?? "")"
        return "\(count)|\(fst)|\(lst)"
    }

    private static func wordTimingSignature(_ tags: [[String: Any]]) -> String {
        guard !tags.isEmpty else { return "0" }
        let count = tags.count
        let first = tags.first
        let last = tags.last
        let fst = "\(first?["t"] ?? -1)-\(first?["i"] ?? -1)"
        let lst = "\(last?["t"] ?? -1)-\(last?["i"] ?? -1)"
        return "\(count)|\(fst)|\(lst)"
    }

    private func cachedStateJSONIfValid(cacheKey: String, includeFullLines: Bool) -> Data? {
        stateCacheLock.lock()
        defer { stateCacheLock.unlock() }
        if includeFullLines {
            guard cachedFullKey == cacheKey else { return nil }
            return cachedFullData
        }
        guard cachedCompactKey == cacheKey else { return nil }
        return cachedCompactData
    }

    private func storeCachedStateJSON(_ data: Data, cacheKey: String, includeFullLines: Bool) {
        stateCacheLock.lock()
        defer { stateCacheLock.unlock() }
        if includeFullLines {
            cachedFullKey = cacheKey
            cachedFullData = data
        } else {
            cachedCompactKey = cacheKey
            cachedCompactData = data
        }
    }

    private func registerStateRequest(cacheHit: Bool) {
        stateCacheLock.lock()
        stateRequestCount += 1
        if cacheHit { stateCacheHitCount += 1 }
        let shouldLog = stateRequestCount % 120 == 0
        let requests = stateRequestCount
        let hits = stateCacheHitCount
        stateCacheLock.unlock()
        if shouldLog {
            log("Flexbar HTTP: state requests=\(requests), cacheHits=\(hits)")
        }
    }

    private func queueSpotifyArtworkFetchIfNeeded(artworkURLString: String, trackID: String) {
        if Self.artworkJPEGBase64Cache.object(forKey: trackID as NSString) != nil { return }

        spotifyArtworkLock.lock()
        if spotifyArtworkPendingTrackIDs.contains(trackID) {
            spotifyArtworkLock.unlock()
            return
        }
        spotifyArtworkPendingTrackIDs.insert(trackID)
        spotifyArtworkLock.unlock()

        guard let url = URL(string: artworkURLString),
              let scheme = url.scheme?.lowercased(),
              scheme == "https" || scheme == "http" else {
            spotifyArtworkLock.lock()
            spotifyArtworkPendingTrackIDs.remove(trackID)
            spotifyArtworkLock.unlock()
            return
        }

        var req = URLRequest(url: url, cachePolicy: .returnCacheDataElseLoad, timeoutInterval: 10)
        req.setValue(
            "Mozilla/5.0 (Macintosh; Intel Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) LyricsXForFlexbar",
            forHTTPHeaderField: "User-Agent"
        )
        let task = URLSession.shared.dataTask(with: req) { data, _, _ in
            defer {
                self.spotifyArtworkLock.lock()
                self.spotifyArtworkPendingTrackIDs.remove(trackID)
                self.spotifyArtworkLock.unlock()
            }
            guard let data, !data.isEmpty, let image = NSImage(data: data) else { return }
            guard let b64 = Self.jpegBase64Artwork(from: image) else { return }
            Self.artworkJPEGBase64Cache.setObject(b64 as NSString, forKey: trackID as NSString)
        }
        task.resume()
    }

    /// Spotify (and occasionally others) may leave `MusicTrack.duration` unset while SB still exposes `duration`.
    private static func resolvedDurationSeconds(for track: MusicTrack) -> TimeInterval {
        let raw: TimeInterval
        if let d = track.duration, d > 0 {
            raw = d
        } else if let o = track.originalTrack as? NSObject,
                  o.responds(to: NSSelectorFromString("duration")) {
            let v = o.value(forKey: "duration")
            if let n = v as? NSNumber, n.doubleValue > 0 {
                raw = n.doubleValue
            } else if let i = v as? Int, i > 0 {
                raw = TimeInterval(i)
            } else {
                return 0
            }
        } else {
            return 0
        }
        // Some SB stacks return track length in milliseconds; values > ~2h as “seconds” are implausible.
        if raw > 7200 {
            return raw / 1000.0
        }
        return raw
    }

    /// In-memory cache: track id → JPEG base64 (Apple Music SB + encode, or Spotify URL fetch).
    private static let artworkJPEGBase64Cache: NSCache<NSString, NSString> = {
        let c = NSCache<NSString, NSString>()
        c.countLimit = 48
        return c
    }()

    /// Cache for furigana runs on current lyric line.
    private static var currentLineReadingCacheKey: NSString?
    private static var currentLineReadingCacheValue: [[String: Any]] = []

    /// Build ruby runs using the same tokenizer + furigana extraction path as desktop lyrics.
    private static func currentLineReading(for line: String, languageCode: String?) -> [[String: Any]] {
        let key = "\(languageCode ?? "")\0\(line)" as NSString
        if let cachedKey = currentLineReadingCacheKey, cachedKey == key {
            return currentLineReadingCacheValue
        }

        var runs: [[String: Any]] = []
        if !line.isEmpty, languageCode?.hasPrefix("ja") == true {
            let nsLine = line as NSString
            let tokenizer = CFStringTokenizer.create(string: .from(nsLine))
            for tokenType in IteratorSequence(tokenizer) where tokenType.contains(.isCJWordMask) {
                guard let (reading, range) = tokenizer.currentFuriganaAnnotation(in: nsLine),
                      range.length > 0 else {
                    continue
                }
                runs.append([
                    "start": range.location,
                    "end": range.location + range.length,
                    "read": reading as String,
                ])
            }
        }

        currentLineReadingCacheKey = key
        currentLineReadingCacheValue = runs
        return runs
    }

    /// Current-line karaoke timing tags from original timetag data.
    /// `t` is seconds relative to line start, `i` is character index in line content.
    private static func currentLineWordTiming(lyrics: Lyrics, lineIndex: Int) -> [[String: Any]] {
        guard lyrics.lines.indices.contains(lineIndex),
              let timetag = lyrics.lines[lineIndex].attachments.timetag else {
            return []
        }
        return timetag.tags
            .map { ["t": $0.time, "i": $0.index] }
            .sorted {
                let t0 = ($0["t"] as? TimeInterval) ?? 0
                let t1 = ($1["t"] as? TimeInterval) ?? 0
                if t0 == t1 {
                    let i0 = ($0["i"] as? Int) ?? 0
                    let i1 = ($1["i"] as? Int) ?? 0
                    return i0 < i1
                }
                return t0 < t1
            }
    }

    /// Reads cover art from the Scripting Bridge `artworks` collection when present (e.g. Music.app).
    /// MusicPlayer’s `LXScriptingTrack` caches `artwork`; if the first read returns empty (common while
    /// Apple Music is still resolving art), that nil is cached for the lifetime of the wrapper — Flexbar
    /// polling would never see a cover. Direct KVC avoids that stale cache.
    private static func nsImageFromScriptingArtworksIfPresent(originalTrack: AnyObject?) -> NSImage? {
        guard let original = originalTrack as? NSObject else { return nil }
        guard original.responds(to: NSSelectorFromString("artworks")) else { return nil }
        guard let rawArtworks = original.value(forKey: "artworks") as? NSArray, rawArtworks.count > 0 else { return nil }
        for idx in 0 ..< rawArtworks.count {
            guard let art = rawArtworks[idx] as? NSObject else { continue }
            if let img = art.value(forKey: "data") as? NSImage,
               img.size.width > 0, img.size.height > 0 {
                return img
            }
            if let rawData = art.value(forKey: "rawData") {
                let data: Data?
                if let d = rawData as? Data {
                    data = d
                } else if let d = rawData as? NSData {
                    data = d as Data
                } else {
                    data = nil
                }
                if let data, let img = NSImage(data: data), img.size.width > 0, img.size.height > 0 {
                    return img
                }
            }
        }
        return nil
    }

    /// Spotify exposes `artworkUrl` on `SpotifyTrack` while `artwork` is always nil (see Spotify.h in MusicPlayer).
    /// This must run on main thread because it touches Scripting Bridge / KVC objects.
    private static func spotifyArtworkURLString(for track: MusicTrack) -> String? {
        guard let original = track.originalTrack as? NSObject else { return nil }
        let artworkURLSelector = NSSelectorFromString("artworkUrl")
        guard original.responds(to: artworkURLSelector) else { return nil }
        guard let raw = original.value(forKey: "artworkUrl") as? String else { return nil }
        let trimmed = raw.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else { return nil }
        return trimmed
    }

    /// JPEG (base64, no data-URL prefix) for Flexbar plugin; downscaled to limit payload size.
    private static func jpegBase64Artwork(from image: NSImage?) -> String? {
        guard let image else { return nil }
        let src = image.size
        guard src.width > 0, src.height > 0 else { return nil }
        let maxSide: CGFloat = 320
        let scale = min(1, maxSide / max(src.width, src.height))
        let target = NSSize(width: max(1, src.width * scale), height: max(1, src.height * scale))
        let rendered = NSImage(size: target, flipped: false) { rect in
            image.draw(
                in: rect,
                from: NSRect(origin: .zero, size: src),
                operation: .sourceOver,
                fraction: 1.0
            )
            return true
        }
        guard let tiff = rendered.tiffRepresentation,
              let rep = NSBitmapImageRep(data: tiff),
              let jpeg = rep.representation(using: .jpeg, properties: [.compressionFactor: 0.82]) else {
            return nil
        }
        return jpeg.base64EncodedString()
    }

    private func writeJSONResponse(fd: Int32, json: Data) {
        let header = """
        HTTP/1.1 200 OK\r
        Content-Type: application/json; charset=utf-8\r
        Content-Length: \(json.count)\r
        Connection: close\r
        \r

        """
        var bytes = Data(header.utf8)
        bytes.append(json)
        _ = bytes.withUnsafeBytes { ptr in
            send(fd, ptr.baseAddress, bytes.count, 0)
        }
    }

    private func writeTextResponse(fd: Int32, status: Int, body: String) {
        let phrase: String
        switch status {
        case 400: phrase = "Bad Request"
        case 404: phrase = "Not Found"
        case 405: phrase = "Method Not Allowed"
        default: phrase = "OK"
        }
        let data = Data(body.utf8)
        let header = """
        HTTP/1.1 \(status) \(phrase)\r
        Content-Type: text/plain; charset=utf-8\r
        Content-Length: \(data.count)\r
        Connection: close\r
        \r

        """
        var bytes = Data(header.utf8)
        bytes.append(data)
        _ = bytes.withUnsafeBytes { ptr in
            send(fd, ptr.baseAddress, bytes.count, 0)
        }
    }
}
