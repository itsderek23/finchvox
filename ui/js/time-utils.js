/**
 * Time Utilities for FinchVox UI
 *
 * Provides standardized time duration formatting across the application.
 */

/**
 * Format a duration in milliseconds to a human-readable string.
 *
 * Formatting rules:
 * - < 1ms: show in ms with 2 decimal places (e.g., "0.05ms")
 * - >= 1ms and < 1s: show in ms with no decimal places (e.g., "250ms")
 * - >= 1s and < 1min: show in seconds with configurable decimals (e.g., "12.3s")
 * - >= 1min and < 1h: show as M:SS + "m" (e.g., "1:02m")
 * - >= 1h and < 24h: show as H:MM:SS + "h" (e.g., "1:02:03h")
 * - >= 24h: show as "Xd H:MM:SS" + "h" (e.g., "1d 1:30:15h")
 *
 * @param {number} milliseconds - Duration in milliseconds
 * @param {number} decimalPlaces - Number of decimal places for seconds display (default: 1)
 * @returns {string} Formatted duration string
 * @throws {Error} If milliseconds is null, undefined, or NaN
 */
function formatDuration(milliseconds, decimalPlaces = 1) {
    // Validate input
    if (milliseconds == null || isNaN(milliseconds)) {
        throw new Error(`Invalid duration input: ${milliseconds}`);
    }

    // Handle zero
    if (milliseconds === 0) {
        return "0ms";
    }

    // Handle negative durations
    const isNegative = milliseconds < 0;
    const absMs = Math.abs(milliseconds);

    let result;

    // < 1ms: show with 2 decimal places
    if (absMs < 1) {
        result = `${absMs.toFixed(2)}ms`;
    }
    // >= 1ms and < 1s: show in ms with no decimals
    else if (absMs < 1000) {
        result = `${Math.round(absMs)}ms`;
    }
    // >= 1s and < 1min: show in seconds with configurable decimals
    else if (absMs < 60000) {
        const seconds = absMs / 1000;
        result = `${seconds.toFixed(decimalPlaces)}s`;
    }
    // >= 1min and < 1h: show as M:SS + "m"
    else if (absMs < 3600000) {
        const totalSeconds = Math.round(absMs / 1000);
        const minutes = Math.floor(totalSeconds / 60);
        const seconds = totalSeconds % 60;
        result = `${minutes}:${String(seconds).padStart(2, '0')}m`;
    }
    // >= 1h and < 24h: show as H:MM:SS + "h"
    else if (absMs < 86400000) {
        const totalSeconds = Math.round(absMs / 1000);
        const hours = Math.floor(totalSeconds / 3600);
        const minutes = Math.floor((totalSeconds % 3600) / 60);
        const seconds = totalSeconds % 60;
        result = `${hours}:${String(minutes).padStart(2, '0')}:${String(seconds).padStart(2, '0')}h`;
    }
    // >= 24h: show as "Xd H:MM:SS" + "h"
    else {
        const totalSeconds = Math.round(absMs / 1000);
        const days = Math.floor(totalSeconds / 86400);
        const hours = Math.floor((totalSeconds % 86400) / 3600);
        const minutes = Math.floor((totalSeconds % 3600) / 60);
        const seconds = totalSeconds % 60;
        result = `${days}d ${hours}:${String(minutes).padStart(2, '0')}:${String(seconds).padStart(2, '0')}h`;
    }

    // Add negative sign if needed
    return isNegative ? `-${result}` : result;
}

/**
 * Format a Unix timestamp to a human-readable date string.
 *
 * @param {number} timestamp - Unix timestamp in seconds
 * @returns {string} Formatted date string (e.g., "Jan 2, 05:44 PM")
 */
function formatDate(timestamp) {
    if (!timestamp) return '';
    const date = new Date(timestamp * 1000);  // Convert seconds to milliseconds
    return date.toLocaleString('en-US', {
        month: 'short',
        day: 'numeric',
        hour: '2-digit',
        minute: '2-digit'
    });
}
