function environmentViewMixin() {
    return {
        environmentData: null,
        environmentLoading: false,
        environmentError: null,
        environmentLoaded: false,

        async loadEnvironmentIfNeeded() {
            if (this.environmentLoaded) return;

            this.environmentLoading = true;
            this.environmentError = null;

            try {
                const response = await fetch(`/api/sessions/${this.sessionId}/environment`);
                if (response.status === 404) {
                    this.environmentError = "No environment data available for this session";
                } else if (!response.ok) {
                    this.environmentError = "Failed to load environment data";
                } else {
                    this.environmentData = await response.json();
                }
            } catch (error) {
                this.environmentError = "Failed to load environment data";
            } finally {
                this.environmentLoading = false;
                this.environmentLoaded = true;
            }
        },

        formatOS() {
            if (!this.environmentData?.os) return "Unknown";
            const os = this.environmentData.os;
            return `${os.system} ${os.release} (${os.machine})`;
        },

        formatPython() {
            if (!this.environmentData?.python) return "Unknown";
            const py = this.environmentData.python;
            return `${py.version} (${py.implementation})`;
        },

        getSortedPackages() {
            if (!this.environmentData?.packages) return {};
            const entries = Object.entries(this.environmentData.packages);
            entries.sort((a, b) => a[0].toLowerCase().localeCompare(b[0].toLowerCase()));
            return Object.fromEntries(entries);
        }
    };
}
