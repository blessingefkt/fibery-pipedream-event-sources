const FiberyAdapter = require('https://github.com/blessingefkt/fibery-pipedream-event-sources/fibery.adapter.js');

let adapterInstance;
module.exports = {
    type: "app",
    app: "fibery",
    methods: {
        adapter() {
            if (!adapterInstance)
                adapterInstance = new FiberyAdapter(this.$auth || {});
            return adapterInstance;
        }
    },
}
