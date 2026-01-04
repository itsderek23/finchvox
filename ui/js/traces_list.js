function tracesListApp() {
    return {
        traces: [],
        dataDir: '',

        async init() {
            await this.loadTraces();
        },

        async loadTraces() {
            try {
                const response = await fetch('/api/traces');
                const data = await response.json();
                this.traces = data.traces || [];
                this.dataDir = data.data_dir || '';
            } catch (error) {
                console.error('Failed to load traces:', error);
            }
        },

        formatDuration(milliseconds) {
            if (!milliseconds) return '-';
            return formatDuration(milliseconds, 0);  // Uses time-utils.js
        }
    };
}
