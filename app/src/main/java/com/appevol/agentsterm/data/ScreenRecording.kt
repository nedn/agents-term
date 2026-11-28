package com.appevol.agentsterm.data

import java.io.File
import java.io.RandomAccessFile

/*
 * Remote screen recording: lets agents on the VM record a video of the phone screen while they
 * drive the app (`agents_term_app_cli record start|stop|status`).
 *
 * The video is captured by Android's own screen capture (MediaProjection), so the user approves
 * every recording in a system dialog and it shows the whole screen (keyboard and system bars
 * included). The finished MP4 stays on the phone; the CLI pulls it in Base64 chunks with
 * `record_read` over the same command channel as everything else.
 *
 * This file is plain Kotlin (unit-testable on the JVM); ScreenRecorder hides the Android parts.
 */

/** A recording that is running or has finished. */
data class ScreenRecording(
    val id: String,
    val startedAtMs: Long,
    val width: Int,
    val height: Int,
    val maxDurationMs: Long,
    val file: File,
    /** Null while recording. */
    val durationMs: Long? = null,
    /** Why it ended (`stopped`, `max_duration`, `user_stopped`, …); null while recording. */
    val stopReason: String? = null,
) {
    val isFinished: Boolean get() = stopReason != null
}

/** Captures the screen. Failures the agent should read are thrown as [AppStateCommandException]. */
interface ScreenRecorder {
    /**
     * Asks the user to approve the capture and returns once recording has begun. The recording
     * ends by itself after [maxDurationMs]. Starting discards the previous recording.
     */
    suspend fun start(maxDurationMs: Long): ScreenRecording

    /** Ends the running recording, or returns the one that already ended by itself. */
    suspend fun stop(): ScreenRecording

    /** The running or last finished recording, if any. */
    fun current(): ScreenRecording?
}

/** Turns `record_start` / `record_stop` / `record_status` / `record_read` into [ScreenRecorder] calls. */
class ScreenRecordingCommands(
    private val recorder: ScreenRecorder,
    private val encodeBase64: (ByteArray) -> String,
    private val clockMs: () -> Long = { System.currentTimeMillis() },
) {
    suspend fun handle(command: String, args: Map<String, Any?>): Map<String, Any?> = when (command) {
        "record_start" -> {
            val seconds = (args["maxDurationSec"] as? Number)?.toLong() ?: DEFAULT_MAX_DURATION_SEC
            describe(recorder.start(seconds.coerceIn(1, MAX_DURATION_SEC) * 1000))
        }

        "record_stop" -> describe(recorder.stop())

        "record_status" -> recorder.current()?.let { describe(it) } ?: mapOf("state" to "idle")

        "record_read" -> read(args)

        else -> throw AppStateCommandException("Unknown recording command '$command'")
    }

    private fun describe(recording: ScreenRecording): Map<String, Any?> = mapOf(
        "state" to if (recording.isFinished) "finished" else "recording",
        "recordingId" to recording.id,
        "startedAt" to recording.startedAtMs,
        "durationMs" to (recording.durationMs ?: (clockMs() - recording.startedAtMs).coerceAtLeast(0)),
        "maxDurationMs" to recording.maxDurationMs,
        "width" to recording.width,
        "height" to recording.height,
        "stopReason" to recording.stopReason,
        // The file is still being written while recording.
        "bytes" to if (recording.isFinished) recording.file.length() else null,
    )

    private fun read(args: Map<String, Any?>): Map<String, Any?> {
        val recording = recorder.current()?.takeIf { it.isFinished }
            ?: throw AppStateCommandException("There is no finished recording to read; run record_stop first.")
        val wantedId = args["recordingId"] as? String
        if (wantedId != null && wantedId != recording.id) {
            throw AppStateCommandException("Recording '$wantedId' is gone; a newer one replaced it.")
        }
        val total = recording.file.length()
        val offset = ((args["offset"] as? Number)?.toLong() ?: 0L).coerceIn(0, total)
        val wanted = ((args["length"] as? Number)?.toInt() ?: MAX_CHUNK_BYTES).coerceIn(1, MAX_CHUNK_BYTES)
        val chunk = ByteArray(minOf(wanted.toLong(), total - offset).toInt())
        RandomAccessFile(recording.file, "r").use { input ->
            input.seek(offset)
            input.readFully(chunk)
        }
        return mapOf(
            "recordingId" to recording.id,
            "offset" to offset,
            "length" to chunk.size,
            "totalBytes" to total,
            "eof" to (offset + chunk.size >= total),
            "dataBase64" to encodeBase64(chunk),
        )
    }

    companion object {
        val COMMANDS = listOf("record_start", "record_stop", "record_status", "record_read")

        const val DEFAULT_MAX_DURATION_SEC = 60L
        const val MAX_DURATION_SEC = 600L

        /** Largest chunk `record_read` returns: small enough for one WebSocket message. */
        const val MAX_CHUNK_BYTES = 512 * 1024
    }
}
