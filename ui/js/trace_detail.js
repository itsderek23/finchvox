function traceDetailApp() {
    return {
        // State
        traceId: null,
        serviceName: null,      // Service name from first span with resource.attributes
        spans: [],              // Original spans from API
        waterfallSpans: [],     // Flat array in display order for waterfall view
        expandedSpanIds: new Set(), // Set of span IDs that are expanded
        expansionInitialized: false, // Flag to ensure we only auto-expand on first load
        isWaterfallExpanded: false, // Global expand/collapse state for the waterfall
        selectedSpan: null,      // Span shown in the details panel
        highlightedSpan: null,   // Span highlighted in the waterfall (for keyboard navigation)
        hoveredSpan: null,       // Span being hovered over (for chunk highlighting)
        chunkHoveredSpan: null,  // Span hovered from chunk (applies .selected to waterfall)
        isPanelOpen: false,      // Controls panel visibility and transitions

        // Audio state
        wavesurfer: null,
        playing: false,
        currentTime: 0,
        duration: 0,
        audioError: false,

        // Copy state
        spanCopied: false,

        // Timeline state
        minTime: 0,
        maxTime: 0,

        // Real-time polling state
        isPolling: false,
        pollInterval: null,
        lastSpanCount: 0,
        consecutiveErrors: 0,

        // Hover marker state
        hoverMarker: {
            visible: false,
            time: 0,
            source: null  // 'waveform' or 'waterfall'
        },

        async init() {
            // Extract trace_id from URL path: /traces/{trace_id}
            const pathParts = window.location.pathname.split('/');
            this.traceId = pathParts[pathParts.length - 1];

            if (!this.traceId) {
                console.error('No trace ID in URL');
                return;
            }

            await this.loadTraceData();

            // Start polling if trace appears to be active
            const conversationSpan = this.spans.find(s => s.name === 'conversation');
            const shouldPoll = !conversationSpan || // No conversation span yet - might be created later
                               (conversationSpan && !conversationSpan.end_time_unix_nano); // Conversation exists but not ended

            if (shouldPoll) {
                this.startPolling();
            }

            this.initAudioPlayer();
            this.initKeyboardShortcuts();
            this.initCleanup();
        },

        async loadTraceData() {
            try {
                const response = await fetch(`/api/trace/${this.traceId}`);
                const data = await response.json();

                // Parse and enrich spans
                this.spans = data.spans.map(span => ({
                    ...span,
                    startMs: Number(span.start_time_unix_nano) / 1_000_000,
                    endMs: Number(span.end_time_unix_nano) / 1_000_000,
                    durationMs: (Number(span.end_time_unix_nano) - Number(span.start_time_unix_nano)) / 1_000_000
                }));

                // Extract service name from first span with resource attributes
                for (const span of this.spans) {
                    if (span.resource && span.resource.attributes) {
                        const serviceAttr = span.resource.attributes.find(attr => attr.key === 'service.name');
                        if (serviceAttr && serviceAttr.value && serviceAttr.value.string_value) {
                            this.serviceName = serviceAttr.value.string_value;
                            break;
                        }
                    }
                }

                // Calculate timeline bounds
                this.minTime = Math.min(...this.spans.map(s => s.startMs));
                this.maxTime = Math.max(...this.spans.map(s => s.endMs));

                // Build waterfall tree structure
                this.buildWaterfallTree();

            } catch (error) {
                console.error('Failed to load trace:', error);
            }
        },

        buildWaterfallTree() {
            // Step 1: Build parent-child map
            const childrenMap = {}; // spanId -> [child spans]
            const rootSpans = [];
            const spanIds = new Set();

            // Track all span IDs for orphan detection
            this.spans.forEach(span => {
                spanIds.add(span.span_id_hex);
            });

            this.spans.forEach(span => {
                const parentId = span.parent_span_id_hex;

                // Check if this is a root span or orphaned span
                const isOrphan = parentId && !spanIds.has(parentId);
                const isRoot = !parentId;

                if (isRoot || isOrphan) {
                    rootSpans.push(span);
                } else if (parentId) {
                    if (!childrenMap[parentId]) {
                        childrenMap[parentId] = [];
                    }
                    childrenMap[parentId].push(span);
                }
            });

            // Step 2: Sort children by start time
            Object.values(childrenMap).forEach(children => {
                children.sort((a, b) => a.startMs - b.startMs);
            });

            // Step 3: Traverse tree depth-first and flatten to display order
            this.waterfallSpans = [];

            const traverse = (span, depth) => {
                span.depth = depth;
                span.children = childrenMap[span.span_id_hex] || [];
                span.childCount = span.children.length;

                // Add this span to the display list
                this.waterfallSpans.push(span);

                // If expanded, add children recursively
                const isExpanded = this.expandedSpanIds.has(span.span_id_hex);
                if (isExpanded && span.children.length > 0) {
                    span.children.forEach(child => traverse(child, depth + 1));
                }
            };

            // Step 4: Start traversal from root spans (sorted by start time)
            rootSpans.sort((a, b) => a.startMs - b.startMs);
            rootSpans.forEach(span => traverse(span, 0));

            // Step 5: Initialize expansion state (collapsed by default - only show conversation and turns)
            // Only run this on first load, not on subsequent rebuilds
            if (!this.expansionInitialized) {
                let addedExpansions = false;

                this.spans.forEach(span => {
                    // Only expand conversation spans by default
                    // This shows conversation and its children (turns), but not children of turns
                    if (span.name === 'conversation') {
                        const children = childrenMap[span.span_id_hex] || [];
                        if (children.length > 0 && !this.expandedSpanIds.has(span.span_id_hex)) {
                            this.expandedSpanIds.add(span.span_id_hex);
                            addedExpansions = true;
                        }
                    }
                });

                // If we just initialized spans as expanded, rebuild the tree
                if (addedExpansions) {
                    this.waterfallSpans = [];
                    rootSpans.forEach(span => traverse(span, 0));
                }

                this.expansionInitialized = true;
                this.isWaterfallExpanded = false; // Start in collapsed state
            }
        },

        toggleSpanExpansion(span) {
            if (span.childCount === 0) {
                // No children, just select the span
                this.selectSpan(span);
                return;
            }

            // Toggle expansion state
            if (this.expandedSpanIds.has(span.span_id_hex)) {
                this.expandedSpanIds.delete(span.span_id_hex);
            } else {
                this.expandedSpanIds.add(span.span_id_hex);
            }

            // Rebuild the waterfall tree
            this.buildWaterfallTree();
        },

        toggleWaterfallExpansion() {
            if (this.isWaterfallExpanded) {
                this.collapseAll();
            } else {
                this.expandAll();
            }
        },

        expandAll() {
            // Expand all spans with children
            this.spans.forEach(span => {
                // Find children for this span
                const children = this.spans.filter(s => s.parent_span_id_hex === span.span_id_hex);
                if (children.length > 0) {
                    this.expandedSpanIds.add(span.span_id_hex);
                }
            });

            this.isWaterfallExpanded = true;
            this.buildWaterfallTree();
        },

        collapseAll() {
            // Collapse everything below turns (depth 2+)
            // Only expand conversation to show turns, but don't expand turns
            this.expandedSpanIds.clear();

            // Find all conversation spans and expand them (this shows turns but not their children)
            this.spans.forEach(span => {
                if (span.name === 'conversation') {
                    const children = this.spans.filter(s => s.parent_span_id_hex === span.span_id_hex);
                    if (children.length > 0) {
                        this.expandedSpanIds.add(span.span_id_hex);
                    }
                }
            });

            this.isWaterfallExpanded = false;
            this.buildWaterfallTree();
        },


        getTimelineBarStyle(span) {
            const totalDuration = this.maxTime - this.minTime;
            const startPercent = ((span.startMs - this.minTime) / totalDuration) * 100;
            const durationPercent = (span.durationMs / totalDuration) * 100;
            const widthPercent = Math.max(durationPercent, 0.15); // Minimum 0.15% for visibility

            return {
                left: `${startPercent}%`,
                width: `${widthPercent}%`,
                isShort: durationPercent < 2
            };
        },

        getTimelineBarClasses(span) {
            const style = this.getTimelineBarStyle(span);
            return {
                [`bar-${span.name}`]: true,
                'short-bar': style.isShort
            };
        },

        getExpandButtonStyle(span) {
            const barStyle = this.getTimelineBarStyle(span);
            const startPercent = parseFloat(barStyle.left);

            // Position button 26px to the left of the bar (16px button + 10px gap)
            // But ensure it doesn't go below 2px from the left edge
            if (startPercent < 3) {
                // For spans starting near 0%, position button at the start of the timeline (2px)
                return {
                    left: '2px'
                };
            }

            return {
                left: `calc(${startPercent}% - 26px)`
            };
        },

        handleRowClick(span) {
            // Expand span children if not already expanded
            if (span.childCount > 0 && !this.expandedSpanIds.has(span.span_id_hex)) {
                this.expandedSpanIds.add(span.span_id_hex);
                this.buildWaterfallTree();
            }

            this.selectSpan(span, true);  // Always seek audio when clicking
        },

        initAudioPlayer() {
            // Only initialize if not already created
            if (this.wavesurfer) {
                return;
            }

            this.wavesurfer = WaveSurfer.create({
                container: '#waveform',
                waveColor: '#a855f7',      // Purple (fallback)
                progressColor: '#7c3aed',  // Darker purple (fallback)
                cursorColor: '#ffffff',
                height: 40,
                barWidth: 2,
                barGap: 1,
                barRadius: 2,
                normalize: true,
                backend: 'WebAudio',
                splitChannels: [
                    {
                        waveColor: getComputedStyle(document.documentElement).getPropertyValue('--span-stt').trim(),
                        progressColor: getComputedStyle(document.documentElement).getPropertyValue('--span-stt-progress').trim()
                    },
                    {
                        waveColor: '#a855f7',      // Purple for channel 1 (bot)
                        progressColor: '#7c3aed'   // Darker purple
                    }
                ]
            });

            this.wavesurfer.load(`/api/audio/${this.traceId}`);

            // Event listeners
            this.wavesurfer.on('play', () => { this.playing = true; });
            this.wavesurfer.on('pause', () => { this.playing = false; });
            this.wavesurfer.on('audioprocess', (time) => { this.currentTime = time; });
            this.wavesurfer.on('seek', (progress) => {
                this.currentTime = progress * this.duration;
            });
            this.wavesurfer.on('ready', () => {
                this.duration = this.wavesurfer.getDuration();
                console.log('Audio ready, duration:', this.duration);

                // Clear error state when audio loads successfully
                if (this.audioError) {
                    console.log('Audio loaded successfully, clearing error state');
                    this.audioError = false;
                }

                // Generate timeline markers
                this.generateTimeline();

                // Setup hover marker listeners
                this.initWaveformHover();
            });
            this.wavesurfer.on('error', (error) => {
                console.error('Audio loading error:', error);
                this.audioError = true;
            });
        },

        generateTimeline() {
            const timeline = document.getElementById('timeline');
            if (!timeline || !this.duration) return;

            timeline.innerHTML = ''; // Clear existing

            // Show exactly 15 equally-spaced markers
            const markerCount = 15;
            const interval = this.duration / markerCount;

            // Create timeline container with relative positioning
            timeline.style.display = 'block';
            timeline.style.position = 'relative';
            timeline.style.width = '100%';
            timeline.style.height = '20px';

            for (let i = 0; i <= markerCount; i++) {
                const time = i * interval;
                const percent = (time / this.duration) * 100;

                const marker = document.createElement('div');
                marker.className = 'timeline-marker';
                marker.style.position = 'absolute';
                marker.style.left = `${percent}%`;
                marker.style.height = '20px';

                // Create tick mark
                const tick = document.createElement('div');
                tick.style.position = 'absolute';
                tick.style.left = '0';
                tick.style.bottom = '0';
                tick.style.width = '1px';
                tick.style.height = '6px';
                tick.style.backgroundColor = '#6b7280';

                // Create label
                const label = document.createElement('span');
                label.style.position = 'absolute';
                label.style.left = '0';
                label.style.top = '0';

                // Align first label left, last label right, middle labels centered
                if (i === 0) {
                    label.style.transform = 'translateX(0)';
                } else if (i === markerCount) {
                    label.style.transform = 'translateX(-100%)';
                } else {
                    label.style.transform = 'translateX(-50%)';
                }

                label.style.fontSize = '10px';
                label.style.color = '#9ca3af';
                label.style.fontFamily = 'monospace';
                label.textContent = this.formatTimelineLabel(time);

                marker.appendChild(tick);
                marker.appendChild(label);
                timeline.appendChild(marker);
            }
        },

        formatTimelineLabel(seconds) {
            // Convert seconds to milliseconds and use unified formatter with 0 decimals
            return formatDuration(seconds * 1000, 0);
        },

        togglePlay() {
            if (this.wavesurfer) {
                this.wavesurfer.playPause();
            }
        },

        skipBackward(seconds) {
            if (!this.wavesurfer || !this.duration) return;

            const currentTime = this.wavesurfer.getCurrentTime();
            const newTime = Math.max(0, currentTime - seconds);
            const progress = newTime / this.duration;

            this.wavesurfer.seekTo(progress);
        },

        skipForward(seconds) {
            if (!this.wavesurfer || !this.duration) return;

            const currentTime = this.wavesurfer.getCurrentTime();
            const newTime = Math.min(this.duration, currentTime + seconds);
            const progress = newTime / this.duration;

            this.wavesurfer.seekTo(progress);
        },

        initKeyboardShortcuts() {
            // Prevent adding listener multiple times
            if (window.__keyboardShortcutsInitialized) {
                return;
            }
            window.__keyboardShortcutsInitialized = true;

            // Store reference to component context for event listener
            const self = this;

            // Add global keyboard event listener for YouTube-style controls
            document.addEventListener('keydown', (event) => {
                // Check if user is typing in an input field
                const activeElement = document.activeElement;
                const isTyping = activeElement && (
                    activeElement.tagName === 'INPUT' ||
                    activeElement.tagName === 'TEXTAREA' ||
                    activeElement.contentEditable === 'true'
                );

                // Don't process shortcuts if user is typing
                if (isTyping) return;

                // Handle keyboard shortcuts
                switch (event.key) {
                    case ' ': // Space - Play/Pause
                        event.preventDefault();
                        if (self.wavesurfer) {
                            self.wavesurfer.playPause();
                        }
                        break;

                    case 'ArrowLeft': // Left Arrow - Skip backward 5 seconds
                        event.preventDefault();
                        if (self.wavesurfer && self.duration) {
                            const currentTime = self.wavesurfer.getCurrentTime();
                            const newTime = Math.max(0, currentTime - 5);
                            const progress = newTime / self.duration;
                            self.wavesurfer.seekTo(progress);
                        }
                        break;

                    case 'ArrowRight': // Right Arrow - Skip forward 5 seconds
                        event.preventDefault();
                        if (self.wavesurfer && self.duration) {
                            const currentTime = self.wavesurfer.getCurrentTime();
                            const newTime = Math.min(self.duration, currentTime + 5);
                            const progress = newTime / self.duration;
                            self.wavesurfer.seekTo(progress);
                        }
                        break;

                    case 'ArrowUp': // Up Arrow - Navigate to previous span
                        event.preventDefault();
                        self.navigateToPreviousSpan();
                        break;

                    case 'ArrowDown': // Down Arrow - Navigate to next span
                        event.preventDefault();
                        self.navigateToNextSpan();
                        break;

                    case 'Escape': // Escape - Close details panel
                        if (self.selectedSpan) {
                            event.preventDefault();
                            self.closePanel();
                        }
                        break;

                    case 'Enter': // Enter - Select highlighted span (open panel and seek)
                        event.preventDefault();
                        if (self.highlightedSpan) {
                            self.selectSpan(self.highlightedSpan, true);
                        }
                        break;
                }
            });
        },

        initCleanup() {
            // Stop polling when user navigates away
            window.addEventListener('beforeunload', () => {
                if (this.isPolling) {
                    this.stopPolling();
                }
            });
        },

        navigateToNextSpan() {
            if (this.waterfallSpans.length === 0) return;

            const panelWasOpen = this.selectedSpan !== null;
            const currentSpan = this.highlightedSpan || this.selectedSpan;

            if (!currentSpan) {
                // No highlight, start at first span
                const nextSpan = this.waterfallSpans[0];
                this.highlightedSpan = nextSpan;
                if (panelWasOpen) {
                    this.selectedSpan = nextSpan;
                }
                this.navigateToSpan(nextSpan);  // Visual feedback only, no audio seek
            } else {
                // Find current index and move to next
                const currentIndex = this.waterfallSpans.findIndex(
                    s => s.span_id_hex === currentSpan.span_id_hex
                );
                if (currentIndex !== -1 && currentIndex < this.waterfallSpans.length - 1) {
                    const nextSpan = this.waterfallSpans[currentIndex + 1];
                    this.highlightedSpan = nextSpan;
                    if (panelWasOpen) {
                        this.selectedSpan = nextSpan;
                    }
                    this.navigateToSpan(nextSpan);  // Visual feedback only, no audio seek
                }
            }
        },

        navigateToPreviousSpan() {
            if (this.waterfallSpans.length === 0) return;

            const panelWasOpen = this.selectedSpan !== null;
            const currentSpan = this.highlightedSpan || this.selectedSpan;

            if (!currentSpan) {
                // No highlight, start at last span
                const prevSpan = this.waterfallSpans[this.waterfallSpans.length - 1];
                this.highlightedSpan = prevSpan;
                if (panelWasOpen) {
                    this.selectedSpan = prevSpan;
                }
                this.navigateToSpan(prevSpan);  // Visual feedback only, no audio seek
            } else {
                // Find current index and move to previous
                const currentIndex = this.waterfallSpans.findIndex(
                    s => s.span_id_hex === currentSpan.span_id_hex
                );
                if (currentIndex > 0) {
                    const prevSpan = this.waterfallSpans[currentIndex - 1];
                    this.highlightedSpan = prevSpan;
                    if (panelWasOpen) {
                        this.selectedSpan = prevSpan;
                    }
                    this.navigateToSpan(prevSpan);  // Visual feedback only, no audio seek
                }
            }
        },

        navigateToSpan(span) {
            // Show hover marker at span position (visual feedback only)
            this.showMarkerAtSpan(span);

            // Scroll the span into view
            setTimeout(() => {
                this.scrollSpanIntoView(span);
            }, 0);
        },

        seekToSpan(span) {
            // Show hover marker at span position
            this.showMarkerAtSpan(span);

            // Seek audio to span start time if audio is not playing
            if (this.wavesurfer && this.duration) {
                const isPlaying = this.wavesurfer.isPlaying();
                if (!isPlaying) {
                    const audioTime = (span.startMs - this.minTime) / 1000;
                    const progress = audioTime / this.duration;
                    this.wavesurfer.seekTo(progress);
                    // Update currentTime directly for immediate UI feedback
                    this.currentTime = audioTime;
                }
            }

            // Scroll the span into view
            setTimeout(() => {
                this.scrollSpanIntoView(span);
            }, 0);
        },

        selectSpan(span, shouldSeekAudio = false) {
            const panelWasOpen = this.isPanelOpen;

            // Update selected span content (doesn't trigger transition)
            this.selectedSpan = span;
            this.highlightedSpan = span;  // Keep highlight in sync when clicking

            // Open panel if not already open (triggers transition only when opening)
            if (!panelWasOpen && span) {
                this.isPanelOpen = true;
            }

            // Seek audio to span start time if requested
            if (shouldSeekAudio) {
                this.seekToSpan(span);
            }

            // If panel state changed (opened), refresh marker position after transition
            if (!panelWasOpen && span) {
                setTimeout(() => {
                    this.refreshMarkerPosition();
                }, 350); // Wait for CSS transition (0.3s) + small buffer
            }
        },

        closePanel() {
            this.isPanelOpen = false;  // Triggers close transition
            this.selectedSpan = null;  // Clear panel content
            // Refresh marker position after panel closes
            setTimeout(() => {
                this.refreshMarkerPosition();
            }, 350); // Wait for CSS transition (0.3s) + small buffer
        },

        scrollSpanIntoView(span) {
            // Find the DOM element for this span
            const spanElement = document.querySelector(`[data-span-id="${span.span_id_hex}"]`);
            if (spanElement) {
                // Use native scrollIntoView with center alignment
                // This automatically handles edge cases (top/bottom boundaries)
                spanElement.scrollIntoView({
                    behavior: 'smooth',
                    // block: 'center',
                    inline: 'nearest'
                });
            }
        },

        handleSpanClick(span, event, clickedOnBadge = false) {
            if (clickedOnBadge && span.childCount > 0) {
                // Clicked on badge - toggle expansion
                this.toggleSpanExpansion(span);
            } else {
                // Clicked on row - select span and seek audio if not playing
                this.selectSpan(span, true);
            }
        },

        formatTime(seconds) {
            // Convert seconds to milliseconds and use unified formatter
            if (!seconds) return formatDuration(0);
            return formatDuration(seconds * 1000);
        },

        formatSpanDuration(span) {
            // Format span duration using unified formatter
            if (!span) return '';
            return formatDuration(span.durationMs);
        },

        formatRelativeStartTime(span) {
            // Format relative start time using unified formatter
            if (!span) return '';
            const relativeMs = span.startMs - this.minTime;
            return formatDuration(relativeMs);
        },

        formatTimestamp(nanos) {
            if (!nanos) return '';
            const date = new Date(Number(nanos) / 1_000_000);
            return date.toISOString().replace('T', ' ').substring(0, 23);
        },

        formatAttributes(span) {
            if (!span || !span.attributes) return '{}';

            // Flatten attributes array to object
            const attrs = {};
            span.attributes.forEach(attr => {
                const value = attr.value.string_value ||
                             attr.value.int_value ||
                             attr.value.double_value ||
                             attr.value.bool_value;
                attrs[attr.key] = value;
            });

            return JSON.stringify(attrs, null, 2);
        },

        formatResourceAttributes(span) {
            if (!span || !span.resource || !span.resource.attributes) return '{}';

            // Flatten resource attributes array to object
            const attrs = {};
            span.resource.attributes.forEach(attr => {
                const value = attr.value.string_value ||
                             attr.value.int_value ||
                             attr.value.double_value ||
                             attr.value.bool_value;
                attrs[attr.key] = value;
            });

            return JSON.stringify(attrs, null, 2);
        },

        getTranscriptText(span) {
            if (!span || !span.attributes) return '';

            const transcriptAttr = span.attributes.find(attr => attr.key === 'transcript');
            if (!transcriptAttr) return '';

            return transcriptAttr.value.string_value || '';
        },

        getOutputText(span) {
            if (!span || !span.attributes) return '';

            const outputAttr = span.attributes.find(attr => attr.key === 'output');
            if (!outputAttr) return '';

            return outputAttr.value.string_value || '';
        },

        // Get a specific attribute value
        getAttribute(span, key) {
            if (!span || !span.attributes) return null;

            const attr = span.attributes.find(a => a.key === key);
            if (!attr) return null;

            return attr.value.string_value ||
                   attr.value.int_value ||
                   attr.value.double_value ||
                   attr.value.bool_value;
        },

        // Check if a specific attribute exists
        hasAttribute(span, key) {
            return this.getAttribute(span, key) !== null;
        },

        // Format a single attribute value as JSON or plain text
        formatAttributeValue(span, key) {
            const value = this.getAttribute(span, key);
            if (value === null) return '';

            // Try to parse as JSON for pretty printing
            try {
                const parsed = JSON.parse(value);
                return JSON.stringify(parsed, null, 2);
            } catch (e) {
                // If not JSON, return as plain text
                return value;
            }
        },

        // Get TTFB (Time to First Byte) value from span attributes
        getTTFB(span) {
            if (!span || !span.attributes) return null;

            const ttfbAttr = span.attributes.find(a => a.key === 'metrics.ttfb');
            if (!ttfbAttr || !ttfbAttr.value.double_value) return null;

            return ttfbAttr.value.double_value;
        },

        // Format TTFB value for display
        formatTTFB(span) {
            const ttfbSeconds = this.getTTFB(span);
            if (ttfbSeconds === null) return '';

            // Convert seconds to milliseconds and format
            return formatDuration(ttfbSeconds * 1000);
        },

        // Get user-bot latency value from span attributes
        getUserBotLatency(span) {
            if (!span || !span.attributes) return null;

            const latencyAttr = span.attributes.find(a => a.key === 'turn.user_bot_latency_seconds');
            if (!latencyAttr || !latencyAttr.value.double_value) return null;

            return latencyAttr.value.double_value;
        },

        // Check if latency is >= 2 seconds (slow response)
        isSlowLatency(span) {
            const latencySeconds = this.getUserBotLatency(span);
            return latencySeconds !== null && latencySeconds >= 2.0;
        },

        // Format user-bot latency value for display with turtle icon if slow
        formatUserBotLatency(span) {
            const latencySeconds = this.getUserBotLatency(span);
            if (latencySeconds === null) return '';

            // Convert seconds to milliseconds and format
            const formattedTime = formatDuration(latencySeconds * 1000);

            // Add turtle icon if latency >= 2 seconds
            if (latencySeconds >= 2.0) {
                return `${formattedTime} <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 512 512" style="width: 16px; height: 16px; display: inline-block; vertical-align: middle; margin-left: 4px; color: white; fill: currentColor;">
  <path d="M511.325,275.018c-0.416-0.982-0.974-1.799-1.54-2.432c-1.117-1.241-2.199-1.891-3.157-2.382 c-1.808-0.892-3.391-1.274-5.107-1.633c-2.982-0.592-6.348-0.916-10.13-1.183c-5.64-0.4-12.13-0.633-18.419-1.016 c-3.166-0.192-6.29-0.433-9.18-0.734c0.3-1.449,0.474-2.932,0.467-4.432c0.008-3.732-0.975-7.447-2.725-10.896 c-1.757-3.458-4.24-6.698-7.372-9.831c-2.991-2.982-6.69-7.489-10.847-12.979c-7.289-9.613-16.045-22.243-26.233-35.738 c-15.311-20.252-33.847-42.503-56.24-59.93c-11.196-8.714-23.376-16.212-36.63-21.56c-13.246-5.339-27.574-8.505-42.853-8.505 c-23.292-0.008-44.302,7.356-62.796,18.544c-13.896,8.398-26.45,18.935-37.813,30.307c-17.036,17.045-31.44,35.955-43.486,52.45 c-6.023,8.239-11.454,15.878-16.27,22.326c-2.757,3.69-5.314,6.981-7.648,9.763c-0.783-0.741-1.549-1.475-2.283-2.208 c-3.582-3.599-6.489-7.139-8.672-12.03c-2.174-4.89-3.699-11.33-3.706-20.876c-0.009-8.781,1.332-20.143,4.673-34.872 c0.642-2.832,0.95-5.656,0.95-8.43c0-6.448-1.691-12.571-4.573-17.961c-4.323-8.114-11.205-14.653-19.318-19.235 c-8.139-4.574-17.578-7.214-27.316-7.223c-9.863-0.008-20.077,2.79-29.032,9.146c-8.181,5.824-13.979,11.18-17.953,16.495 c-1.974,2.658-3.491,5.315-4.531,8.023C0.542,148.685,0,151.442,0,154.141c-0.008,3.124,0.742,6.106,1.974,8.672 c1.075,2.258,2.491,4.216,4.057,5.906c2.741,2.966,5.94,5.182,9.139,6.998c4.816,2.691,9.722,4.449,13.496,5.599 c0.332,0.1,0.649,0.2,0.974,0.283c1.442,21.226,4.307,38.638,8.081,53.033c6.131,23.392,14.728,38.87,23.317,49.425 c4.282,5.274,8.547,9.305,12.346,12.462c3.799,3.158,7.156,5.474,9.464,7.215c5.465,4.098,10.696,7.047,15.687,8.996 c3.673,1.433,7.223,2.316,10.613,2.683v0.009c4.799,2.874,16.695,9.555,35.147,16.694c-0.183,0.666-0.5,1.491-0.925,2.4 c-1.124,2.432-2.99,5.464-5.123,8.463c-3.232,4.541-7.089,9.08-10.113,12.437c-1.516,1.675-2.808,3.058-3.724,4.024 c-0.467,0.484-0.816,0.85-1.075,1.084l-0.15,0.166c-0.016,0.017-0.091,0.1-0.2,0.208c-0.792,0.758-3.816,3.69-6.956,7.898 c-1.766,2.4-3.599,5.198-5.074,8.389c-1.458,3.199-2.616,6.798-2.64,10.888c-0.017,2.899,0.666,6.056,2.274,8.93 c0.883,1.608,2.007,2.933,3.224,4.041c2.124,1.958,4.54,3.357,7.09,4.482c3.857,1.699,8.097,2.824,12.546,3.582 c4.448,0.758,9.056,1.124,13.504,1.124c5.298-0.016,10.313-0.5,14.778-1.675c2.233-0.616,4.332-1.39,6.365-2.607 c1.016-0.608,2.008-1.342,2.949-2.308c0.925-0.933,1.808-2.133,2.441-3.599c0.366-0.883,1.1-2.466,2.049-4.44 c3.316-6.94,9.297-18.802,14.404-28.857c2.566-5.04,4.907-9.63,6.606-12.954c0.85-1.674,1.55-3.024,2.033-3.965 c0.475-0.924,0.733-1.442,0.733-1.442l0.016-0.033l0.042-0.042c0.033-0.067,0.075-0.142,0.092-0.217 c23.226,4.758,50.517,8.048,81.565,8.048c1.641,0,3.266,0,4.907-0.025h0.025c23.184-0.274,43.978-2.416,62.23-5.606 c2.25,4.39,7.597,14.812,12.804,25.15c2.657,5.256,5.274,10.497,7.414,14.87c1.092,2.174,2.05,4.148,2.824,5.79 c0.774,1.624,1.383,2.956,1.716,3.723c0.624,1.466,1.491,2.666,2.432,3.599c1.666,1.666,3.433,2.699,5.256,3.507 c2.75,1.2,5.69,1.9,8.84,2.383c3.157,0.475,6.514,0.7,9.98,0.7c6.814-0.016,13.937-0.833,20.318-2.64 c3.174-0.917,6.181-2.083,8.93-3.691c1.383-0.808,2.691-1.732,3.907-2.857c1.199-1.108,2.324-2.433,3.215-4.041 c1.625-2.874,2.283-6.031,2.266-8.93c0-4.09-1.158-7.689-2.616-10.888c-2.215-4.774-5.223-8.722-7.681-11.638 c-2.099-2.457-3.799-4.132-4.374-4.648v-0.016c-0.016-0.026-0.033-0.042-0.05-0.059c-0.024-0.016-0.024-0.033-0.042-0.033 c-0.033-0.042-0.05-0.058-0.091-0.1c-0.991-0.991-5.665-5.806-10.422-11.654c-2.641-3.232-5.274-6.772-7.306-10.039 c-0.7-1.107-1.308-2.199-1.832-3.215c20.868-7.689,33.806-15.295,38.438-18.227c0.883-0.05,1.848-0.125,2.907-0.225 c7.248-0.725,18.752-2.816,30.956-7.847c6.098-2.516,12.354-5.774,18.269-10.022c5.914-4.249,11.488-9.497,16.103-15.953 l0.166-0.242l0.158-0.258c0.341-0.575,0.666-1.241,0.916-2.024c0.241-0.776,0.408-1.683,0.408-2.641 C512,277.21,511.759,276.027,511.325,275.018z"/>
</svg>`;
            }

            return formattedTime;
        },

        // Get interruption status from span attributes
        wasInterrupted(span) {
            if (!span || !span.attributes) return null;

            const interruptedAttr = span.attributes.find(a => a.key === 'turn.was_interrupted');
            if (!interruptedAttr || interruptedAttr.value.bool_value === undefined) return null;

            return interruptedAttr.value.bool_value;
        },

        // Format interruption status for display
        formatInterrupted(span) {
            const interrupted = this.wasInterrupted(span);
            if (interrupted === null) return '';

            if (interrupted) {
                return `Yes <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="currentColor" style="width: 16px; height: 16px; display: inline-block; vertical-align: middle; margin-left: 4px; color: white;">
  <path d="M13.5 4.06c0-1.336-1.616-2.005-2.56-1.06l-4.5 4.5H4.508c-1.141 0-2.318.664-2.66 1.905A9.76 9.76 0 0 0 1.5 12c0 .898.121 1.768.35 2.595.341 1.24 1.518 1.905 2.659 1.905h1.93l4.5 4.5c.945.945 2.561.276 2.561-1.06V4.06ZM17.78 9.22a.75.75 0 1 0-1.06 1.06L18.44 12l-1.72 1.72a.75.75 0 1 0 1.06 1.06l1.72-1.72 1.72 1.72a.75.75 0 1 0 1.06-1.06L20.56 12l1.72-1.72a.75.75 0 1 0-1.06-1.06l-1.72 1.72-1.72-1.72Z" />
</svg>`;
            } else {
                return 'No';
            }
        },

        // Format bot chunk text (interrupt icon removed for cleaner display)
        formatBotChunkText(chunk) {
            if (!chunk.botText) return '';
            return chunk.botText;
        },

        // Format bar duration with interruption and slow latency icons if needed
        formatBarDuration(span) {
            if (!span) return '';

            let result = formatDuration(span.durationMs);

            // Add interrupt icon for interrupted turns
            if (span.name === 'turn' && this.wasInterrupted(span)) {
                result += ` <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="currentColor" style="width: 14px; height: 14px; display: inline-block; vertical-align: top; margin-left: 3px; color: white;">
  <path d="M13.5 4.06c0-1.336-1.616-2.005-2.56-1.06l-4.5 4.5H4.508c-1.141 0-2.318.664-2.66 1.905A9.76 9.76 0 0 0 1.5 12c0 .898.121 1.768.35 2.595.341 1.24 1.518 1.905 2.659 1.905h1.93l4.5 4.5c.945.945 2.561.276 2.561-1.06V4.06ZM17.78 9.22a.75.75 0 1 0-1.06 1.06L18.44 12l-1.72 1.72a.75.75 0 1 0 1.06 1.06l1.72-1.72 1.72 1.72a.75.75 0 1 0 1.06-1.06L20.56 12l1.72-1.72a.75.75 0 1 0-1.06-1.06l-1.72 1.72-1.72-1.72Z" />
</svg>`;
            }

            // Add turtle icon for slow latency (>= 2s)
            if (span.name === 'turn' && this.isSlowLatency(span)) {
                result += ` <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 512 512" style="width: 14px; height: 14px; display: inline-block; vertical-align: top; margin-left: 3px; color: white; fill: currentColor;">
  <path d="M511.325,275.018c-0.416-0.982-0.974-1.799-1.54-2.432c-1.117-1.241-2.199-1.891-3.157-2.382 c-1.808-0.892-3.391-1.274-5.107-1.633c-2.982-0.592-6.348-0.916-10.13-1.183c-5.64-0.4-12.13-0.633-18.419-1.016 c-3.166-0.192-6.29-0.433-9.18-0.734c0.3-1.449,0.474-2.932,0.467-4.432c0.008-3.732-0.975-7.447-2.725-10.896 c-1.757-3.458-4.24-6.698-7.372-9.831c-2.991-2.982-6.69-7.489-10.847-12.979c-7.289-9.613-16.045-22.243-26.233-35.738 c-15.311-20.252-33.847-42.503-56.24-59.93c-11.196-8.714-23.376-16.212-36.63-21.56c-13.246-5.339-27.574-8.505-42.853-8.505 c-23.292-0.008-44.302,7.356-62.796,18.544c-13.896,8.398-26.45,18.935-37.813,30.307c-17.036,17.045-31.44,35.955-43.486,52.45 c-6.023,8.239-11.454,15.878-16.27,22.326c-2.757,3.69-5.314,6.981-7.648,9.763c-0.783-0.741-1.549-1.475-2.283-2.208 c-3.582-3.599-6.489-7.139-8.672-12.03c-2.174-4.89-3.699-11.33-3.706-20.876c-0.009-8.781,1.332-20.143,4.673-34.872 c0.642-2.832,0.95-5.656,0.95-8.43c0-6.448-1.691-12.571-4.573-17.961c-4.323-8.114-11.205-14.653-19.318-19.235 c-8.139-4.574-17.578-7.214-27.316-7.223c-9.863-0.008-20.077,2.79-29.032,9.146c-8.181,5.824-13.979,11.18-17.953,16.495 c-1.974,2.658-3.491,5.315-4.531,8.023C0.542,148.685,0,151.442,0,154.141c-0.008,3.124,0.742,6.106,1.974,8.672 c1.075,2.258,2.491,4.216,4.057,5.906c2.741,2.966,5.94,5.182,9.139,6.998c4.816,2.691,9.722,4.449,13.496,5.599 c0.332,0.1,0.649,0.2,0.974,0.283c1.442,21.226,4.307,38.638,8.081,53.033c6.131,23.392,14.728,38.87,23.317,49.425 c4.282,5.274,8.547,9.305,12.346,12.462c3.799,3.158,7.156,5.474,9.464,7.215c5.465,4.098,10.696,7.047,15.687,8.996 c3.673,1.433,7.223,2.316,10.613,2.683v0.009c4.799,2.874,16.695,9.555,35.147,16.694c-0.183,0.666-0.5,1.491-0.925,2.4 c-1.124,2.432-2.99,5.464-5.123,8.463c-3.232,4.541-7.089,9.08-10.113,12.437c-1.516,1.675-2.808,3.058-3.724,4.024 c-0.467,0.484-0.816,0.85-1.075,1.084l-0.15,0.166c-0.016,0.017-0.091,0.1-0.2,0.208c-0.792,0.758-3.816,3.69-6.956,7.898 c-1.766,2.4-3.599,5.198-5.074,8.389c-1.458,3.199-2.616,6.798-2.64,10.888c-0.017,2.899,0.666,6.056,2.274,8.93 c0.883,1.608,2.007,2.933,3.224,4.041c2.124,1.958,4.54,3.357,7.09,4.482c3.857,1.699,8.097,2.824,12.546,3.582 c4.448,0.758,9.056,1.124,13.504,1.124c5.298-0.016,10.313-0.5,14.778-1.675c2.233-0.616,4.332-1.39,6.365-2.607 c1.016-0.608,2.008-1.342,2.949-2.308c0.925-0.933,1.808-2.133,2.441-3.599c0.366-0.883,1.1-2.466,2.049-4.44 c3.316-6.94,9.297-18.802,14.404-28.857c2.566-5.04,4.907-9.63,6.606-12.954c0.85-1.674,1.55-3.024,2.033-3.965 c0.475-0.924,0.733-1.442,0.733-1.442l0.016-0.033l0.042-0.042c0.033-0.067,0.075-0.142,0.092-0.217 c23.226,4.758,50.517,8.048,81.565,8.048c1.641,0,3.266,0,4.907-0.025h0.025c23.184-0.274,43.978-2.416,62.23-5.606 c2.25,4.39,7.597,14.812,12.804,25.15c2.657,5.256,5.274,10.497,7.414,14.87c1.092,2.174,2.05,4.148,2.824,5.79 c0.774,1.624,1.383,2.956,1.716,3.723c0.624,1.466,1.491,2.666,2.432,3.599c1.666,1.666,3.433,2.699,5.256,3.507 c2.75,1.2,5.69,1.9,8.84,2.383c3.157,0.475,6.514,0.7,9.98,0.7c6.814-0.016,13.937-0.833,20.318-2.64 c3.174-0.917,6.181-2.083,8.93-3.691c1.383-0.808,2.691-1.732,3.907-2.857c1.199-1.108,2.324-2.433,3.215-4.041 c1.625-2.874,2.283-6.031,2.266-8.93c0-4.09-1.158-7.689-2.616-10.888c-2.215-4.774-5.223-8.722-7.681-11.638 c-2.099-2.457-3.799-4.132-4.374-4.648v-0.016c-0.016-0.026-0.033-0.042-0.05-0.059c-0.024-0.016-0.024-0.033-0.042-0.033 c-0.033-0.042-0.05-0.058-0.091-0.1c-0.991-0.991-5.665-5.806-10.422-11.654c-2.641-3.232-5.274-6.772-7.306-10.039 c-0.7-1.107-1.308-2.199-1.832-3.215c20.868-7.689,33.806-15.295,38.438-18.227c0.883-0.05,1.848-0.125,2.907-0.225 c7.248-0.725,18.752-2.816,30.956-7.847c6.098-2.516,12.354-5.774,18.269-10.022c5.914-4.249,11.488-9.497,16.103-15.953 l0.166-0.242l0.158-0.258c0.341-0.575,0.666-1.241,0.916-2.024c0.241-0.776,0.408-1.683,0.408-2.641 C512,277.21,511.759,276.027,511.325,275.018z"/>
</svg>`;
            }

            return result;
        },

        // Get all tool calls from the input attribute
        getToolCalls(span) {
            if (!span || span.name !== 'llm') return [];

            const inputValue = this.getAttribute(span, 'input');
            if (!inputValue) return [];

            try {
                const messages = JSON.parse(inputValue);
                if (!Array.isArray(messages)) return [];

                const toolCalls = [];
                // Iterate through all messages and collect tool calls
                messages.forEach(msg => {
                    if (msg.role === 'assistant' && msg.tool_calls && Array.isArray(msg.tool_calls)) {
                        toolCalls.push(...msg.tool_calls);
                    }
                });

                return toolCalls;
            } catch (e) {
                console.error('Error parsing tool calls:', e);
                return [];
            }
        },

        // Format tool calls as JSON
        formatToolCalls(span) {
            const toolCalls = this.getToolCalls(span);
            if (toolCalls.length === 0) return '[]';

            return JSON.stringify(toolCalls, null, 2);
        },

        // Get raw span JSON (excluding computed properties)
        getRawSpanJSON(span) {
            if (!span) return '{}';

            // Exclude computed properties added by the frontend
            const { startMs, endMs, durationMs, depth, children, childCount, ...rawSpan } = span;

            return JSON.stringify(rawSpan, null, 2);
        },

        // Copy span JSON to clipboard with visual feedback
        async copySpanToClipboard() {
            if (!this.selectedSpan) return;

            try {
                const spanJSON = this.getRawSpanJSON(this.selectedSpan);
                await navigator.clipboard.writeText(spanJSON);

                // Visual feedback: set copied state
                this.spanCopied = true;

                // Reset after 1.5 seconds
                setTimeout(() => {
                    this.spanCopied = false;
                }, 1500);
            } catch (err) {
                console.error('Failed to copy span:', err);
            }
        },

        // Initialize waveform hover listeners
        initWaveformHover() {
            const waveformContainer = document.getElementById('waveform');
            if (!waveformContainer) return;

            waveformContainer.addEventListener('mousemove', (e) => {
                const rect = waveformContainer.getBoundingClientRect();
                const x = e.clientX - rect.left;
                const percent = x / rect.width;
                const time = percent * this.duration;

                this.hoverMarker.time = time;
                this.hoverMarker.source = 'waveform';
                this.hoverMarker.visible = true;
            });

            waveformContainer.addEventListener('mouseleave', () => {
                if (this.hoverMarker.source === 'waveform') {
                    this.hoverMarker.visible = false;
                }
            });
        },

        // Show hover marker at span start time (called from waterfall row hover)
        showMarkerAtSpan(span) {
            // Calculate relative time from trace start (minTime is already in ms)
            const relativeMs = span.startMs - this.minTime;
            this.hoverMarker.time = relativeMs / 1000; // Convert to seconds
            this.hoverMarker.source = 'waterfall';
            this.hoverMarker.visible = true;
        },

        // Hide hover marker (called from waterfall row leave)
        hideMarkerFromWaterfall() {
            if (this.hoverMarker.source === 'waterfall') {
                this.hoverMarker.visible = false;
            }
        },

        // Refresh hover marker position (called when waveform width changes, e.g., panel open/close)
        refreshMarkerPosition() {
            if (this.hoverMarker.visible && this.hoverMarker.source === 'waterfall') {
                // Force Alpine to recalculate by triggering a reactive update
                // We temporarily store the time value, toggle visibility, and restore
                const savedTime = this.hoverMarker.time;
                this.hoverMarker.visible = false;
                this.$nextTick(() => {
                    this.hoverMarker.time = savedTime;
                    this.hoverMarker.visible = true;
                });
            }
        },

        // Get hover marker position in pixels
        getMarkerPosition() {
            if (!this.duration || !this.hoverMarker.visible) return '32px'; // 2rem = 32px

            const waveform = document.getElementById('waveform');
            if (!waveform) return '32px';

            const waveformWidth = waveform.offsetWidth;
            const percent = this.hoverMarker.time / this.duration;
            const offsetInWaveform = percent * waveformWidth;
            const totalOffset = 32 + offsetInWaveform; // 32px = 2rem padding

            return `${totalOffset}px`;
        },

        // Format hover marker time label
        getMarkerTimeLabel() {
            if (!this.hoverMarker.visible) return '';
            return this.formatTime(this.hoverMarker.time);
        },

        // Highlight span when hovering over chunk or waterfall row
        highlightSpan(span) {
            this.hoveredSpan = span;
            this.showMarkerAtSpan(span);
        },

        // Unhighlight span when leaving chunk or waterfall row
        unhighlightSpan() {
            this.hoveredSpan = null;
            this.hideMarkerFromWaterfall();
        },

        // Highlight span from chunk hover (also applies .selected to waterfall)
        highlightSpanFromChunk(span) {
            this.hoveredSpan = span;
            this.chunkHoveredSpan = span;
            this.showMarkerAtSpan(span);
        },

        // Unhighlight span when leaving chunk
        unhighlightSpanFromChunk() {
            this.hoveredSpan = null;
            this.chunkHoveredSpan = null;
            this.hideMarkerFromWaterfall();
        },

        // Handle chunk click - delegates to span click handler and expands children
        handleChunkClick(span) {
            // Expand span children if not already expanded
            if (span.childCount > 0 && !this.expandedSpanIds.has(span.span_id_hex)) {
                this.expandedSpanIds.add(span.span_id_hex);
                this.buildWaterfallTree();
            }

            // Execute normal click behavior
            this.handleRowClick(span);
        },

        getTurnChunks() {
            // Return empty array if no data or audio not ready
            if (!this.spans || this.spans.length === 0 || !this.duration) {
                return [];
            }

            // Find all turn spans
            const turnSpans = this.spans.filter(s => s.name === 'turn');

            // Map each turn to a chunk object with text and positioning
            return turnSpans.map(turn => {
                // Find all STT and LLM children
                const children = this.spans.filter(s => s.parent_span_id_hex === turn.span_id_hex);
                const sttChildren = children.filter(c => c.name === 'stt');
                const llmChildren = children.filter(c => c.name === 'llm');

                // Concatenate text from all STT spans
                const humanText = sttChildren
                    .map(child => this.getTranscriptText(child))
                    .filter(text => text) // Remove empty strings
                    .join(' ');

                // Concatenate text from all LLM spans
                const botText = llmChildren
                    .map(child => this.getOutputText(child))
                    .filter(text => text) // Remove empty strings
                    .join(' ');

                // Calculate positioning (reuses existing method)
                const style = this.getTimelineBarStyle(turn);

                return {
                    span_id_hex: turn.span_id_hex,
                    span: turn,  // Store reference for hover marker
                    humanText: humanText,
                    botText: botText,
                    style: style,  // { left: "X%", width: "Y%" }
                    wasInterrupted: this.wasInterrupted(turn)
                };
            });
        },

        // Real-time polling methods
        startPolling() {
            if (this.isPolling) return;

            this.isPolling = true;
            this.lastSpanCount = this.spans.length;
            console.log('Starting real-time polling for synchronized trace updates');

            // Poll for new spans every 1 second (audio reloads when spans update)
            this.pollInterval = setInterval(() => {
                this.pollForSpans();
            }, 1000);
        },

        stopPolling() {
            if (!this.isPolling) return;

            console.log('Stopping real-time polling');
            this.isPolling = false;
            if (this.pollInterval) clearInterval(this.pollInterval);
            this.pollInterval = null;
        },

        async reloadAudioIfNotPlaying() {
            // Only reload audio if it's not currently playing
            if (this.wavesurfer && !this.wavesurfer.isPlaying()) {
                console.log('Reloading audio waveform (synchronized with spans)');
                // Add cache-busting parameter to force reload
                const audioUrl = `/api/audio/${this.traceId}?t=${Date.now()}`;
                this.wavesurfer.load(audioUrl);
            } else if (this.wavesurfer) {
                console.log('Audio is playing, skipping reload');
            }
        },

        async pollForSpans() {
            try {
                const response = await fetch(`/api/trace/${this.traceId}`);
                if (!response.ok) {
                    throw new Error(`HTTP ${response.status}`);
                }

                const data = await response.json();
                this.consecutiveErrors = 0;

                // Check stopping condition 1: Conversation complete (if conversation span exists)
                const conversationSpan = data.spans.find(s => s.name === 'conversation');
                if (conversationSpan && conversationSpan.end_time_unix_nano) {
                    console.log('Conversation complete, stopping polling');
                    this.stopPolling();
                    return;
                }

                // Check stopping condition 2: Trace abandoned (10 minutes since last span)
                if (data.last_span_time) {
                    const lastSpanMs = Number(data.last_span_time) / 1_000_000;
                    const nowMs = Date.now();
                    const tenMinutesMs = 10 * 60 * 1000;

                    if ((nowMs - lastSpanMs) > tenMinutesMs) {
                        console.log('Trace abandoned (>10 min since last span), stopping polling');
                        this.stopPolling();
                        return;
                    }
                }

                // Check if new spans arrived
                if (data.spans.length > this.lastSpanCount) {
                    console.log(`New spans detected: ${data.spans.length - this.lastSpanCount} new spans`);

                    // Get new spans by comparing span IDs
                    const existingSpanIds = new Set(this.spans.map(s => s.span_id_hex));
                    const newSpans = data.spans.filter(s => !existingSpanIds.has(s.span_id_hex));

                    // Add computed properties to new spans (same as loadTraceData)
                    newSpans.forEach(span => {
                        span.startMs = Number(span.start_time_unix_nano) / 1_000_000;
                        span.endMs = Number(span.end_time_unix_nano) / 1_000_000;
                        span.durationMs = (Number(span.end_time_unix_nano) - Number(span.start_time_unix_nano)) / 1_000_000;
                        this.spans.push(span);
                    });

                    // Update timeline bounds
                    this.minTime = Math.min(...this.spans.map(s => s.startMs));
                    this.maxTime = Math.max(...this.spans.map(s => s.endMs));

                    // Rebuild waterfall tree with new spans
                    this.buildWaterfallTree();

                    // Reload audio waveform (synchronized with span update)
                    await this.reloadAudioIfNotPlaying();

                    // Update count
                    this.lastSpanCount = this.spans.length;
                }

            } catch (error) {
                console.error('Error polling for spans:', error);
                this.consecutiveErrors++;

                // Stop polling after 3 consecutive errors
                if (this.consecutiveErrors >= 3) {
                    console.error('Too many consecutive errors, stopping polling');
                    this.stopPolling();
                }
            }
        }
    };
}
