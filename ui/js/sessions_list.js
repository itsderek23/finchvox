function sessionsListApp() {
    return {
        sessions: [],
        dataDir: '',

        async init() {
            await this.loadSessions();
        },

        async loadSessions() {
            try {
                const response = await fetch('/api/sessions');
                const data = await response.json();
                this.sessions = data.sessions || [];
                this.dataDir = data.data_dir || '';
            } catch (error) {
                console.error('Failed to load sessions:', error);
            }
        },

        formatDuration(milliseconds) {
            if (!milliseconds) return '-';
            return formatDuration(milliseconds, 0);
        },

        formatCount(count) {
            return (count && count > 0) ? count.toLocaleString() : '—';
        }
    };
}
