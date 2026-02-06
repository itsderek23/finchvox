function sessionsListApp() {
    return {
        sessions: [],
        dataDir: '',
        showUploadSection: false,
        uploadFile: null,
        uploading: false,
        uploadError: null,

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

        async uploadSession() {
            if (!this.uploadFile) return;

            this.uploading = true;
            this.uploadError = null;

            try {
                const formData = new FormData();
                formData.append('file', this.uploadFile);

                const response = await fetch('/api/sessions/upload', {
                    method: 'POST',
                    body: formData
                });

                const data = await response.json();

                if (!response.ok) {
                    throw new Error(data.detail || 'Upload failed');
                }

                window.location.href = `/sessions/${data.session_id}`;
            } catch (error) {
                this.uploadError = error.message;
            } finally {
                this.uploading = false;
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
