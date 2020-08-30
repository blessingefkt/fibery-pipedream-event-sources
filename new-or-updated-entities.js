const fibery = require('https://github.com/blessingefkt/fibery-pipedream-event-sources/fibery.app.js');

module.exports = {
    name: "fibery-entities-created-or-updated",
    version: "0.0.7",
    props: {
        fibery,
        db: "$.service.db",
        entityType: {
            description: "ID of an Entity Type",
            type: "string",
            async options() {
                return await this.fibery.typeOptions();
            }
        },
        fields: {
            type: "string[]",
            async options() {
                return await this.fibery.getTypeFieldOptions(this.entityType);
            },
        },
        limit: {
            type: "string",
            default: "10"
        },
        timer: {
            type: "$.interface.timer",
            default: {
                intervalSeconds: 60 * 5,
            },
        },
    },
    async run(event) {
        const entityTypeKey = Buffer.from(`${this.fibery.$auth.account_name}/${this.entityType}`).toString('base64');
        const dbKey = 'lastMaxTimestamp/'+ entityTypeKey;
        const lastMaxTimestamp = this.db.get(dbKey) || new Date().toISOString();
        const queryObject = await this.fibery.getQueryObject(this.entityType, {
            lastMaxTimestamp: lastMaxTimestamp,
            fields: this.fields,
            dateFields: ['fibery/creation-date', 'fibery/modification-date'],
            limit: Number(this.limit)
        });
        const entityType = queryObject.query['q/from'];

        console.log('query', JSON.stringify(queryObject, null, 2));
        let entities = [];

        entities = await this.fibery.queryEntities(queryObject.query, queryObject.params);
        if (!entities.length) {
            console.log(`No new entities.`);
            return;
        }

        const metadata = {entityType, lastMaxTimestamp};

        let maxTimestamp;
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
