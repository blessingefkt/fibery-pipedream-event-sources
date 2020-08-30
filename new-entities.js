const fibery = require('https://github.com/blessingefkt/fibery-pipedream-event-sources/fibery.app.js');
const sortby = require('lodash.sortby');

module.exports = {
    name: "fibery-entities-created",
    version: "0.0.1",
    props: {
        fibery,
        db: "$.service.db",
        entityType: {
            description: "ID of an Entity Type",
            type: "string",
            async options() {
                return sortby(await this.fibery.typeOptions(), ['label']);
            }
        },
        fields: {
            type: "string[]",
            default: [],
            async options() {
                return sortby(await this.fibery.getTypeFieldOptions(this.entityType), ['label']);
            },
        },
        timer: {
            type: "$.interface.timer",
            default: {
                intervalSeconds: 60 * 5,
            },
        },
    },
    async run(event) {
        const query = await this.fibery.getQueryObject(this.entityType, this.fields);
        let maxTimestamp;

        const entityType = query['q/from'];
        const dbKey = `lastMaxTimestamp/${this.$auth.account_name}/${entityType}`;
        const params = {};

        const lastMaxTimestamp = this.db.get(dbKey);
        if (lastMaxTimestamp) {
            params['$lastMaxTimestamp'] = lastMaxTimestamp;
            query['q/where'] = ['>', ['fibery/creation-date'], '$lastMaxTimestamp'];
        }

        console.log('query', JSON.stringify({params, query}, null, 2));
        let entities = [];

        entities = await this.fibery.queryEntities(query, params);
        if (!entities.length) {
            console.log(`No new entities.`);
            return;
        }

        const metadata = {entityType, lastMaxTimestamp};

        const entityIds = [];
        for (let entity of entities) {
            const id = entity['fibery/id'];
            entityIds.push(id);
            const result = {entity, ...metadata, number: entityIds.length};
            this.$emit(result, {
                id,
                ts: entity['fibery/creation-date'],
                summary: JSON.stringify(entity),
            })
            if (!maxTimestamp || entity['fibery/creation-date'] > maxTimestamp) {
                maxTimestamp = entity['fibery/creation-date']
            }
        }
        const entityCount = entityIds.length;
        console.log(`Emitted ${entityCount} new records(s).`, entityIds);
        this.db.set(dbKey, maxTimestamp)
    },
}
