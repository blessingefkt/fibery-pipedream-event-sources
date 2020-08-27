const Fibery = require('fibery-unofficial');

let _host = null;
let _token = null;

async function getEntitiesViaAxios(fibery, query, params) {
    const axios = require('axios');
    const response = await axios.post(`https://${_host}/api/commands`, [
        {
            "command": "fibery.entity/query",
            "args": {params, query}
        }
    ], {
        headers: {
            Authorization: `Token ${_token}`
        }
    });
    const data = response.data[0];
    // console.log('data', data);
    if (data.success) {
        return data.result;
    }
    const errorResult = data.result;
    const error = new Error(errorResult.message + ' - TRACE: ' + errorResult.trace);
    error.details = errorResult;
    throw error;
}

async function getEntitiesViaFibery(fibery, query, params) {
    const client = fibery._client;
    return await client.entity.query(query, params);
}

module.exports = {
    name: "New Fibery Entities Created",
    version: "0.0.1",
    props: {
        db: "$.service.db",
        host: "string",
        token: "string",
        typeId: "string",
        timer: {
            type: "$.interface.timer",
            default: {
                intervalSeconds: 60 * 5,
            },
        },
    },
    async run(event) {
        const {typeId, token, host} = this;
        _token = token;
        _host = host;
        const fibery = {
            get _client() {
                return new Fibery({host, token});
            },
            async _schema() {
                return this._client.getSchema();
            },
            async types() {
                return this._schema().map(type => ({
                    name: type['fibery/name'],
                    id: type['fibery/id'],
                }));
            },
        }
        const params = {};
        const orderBy = [
            [['fibery/creation-date'], 'q/asc']
        ];
        const query = {
            'q/from': typeId,
            'q/where': undefined,
            'q/order-by': orderBy,
            'q/select': [
                'fibery/id',
                'fibery/public-id',
                'fibery/creation-date',
                'fibery/modification-date'
            ],
            'q/limit': 3,
        };

        let maxTimestamp
        const lastMaxTimestamp = this.db.get("lastMaxTimestamp")
        if (lastMaxTimestamp) {
            params['$lastMaxTimestamp'] = lastMaxTimestamp;
            query['q/where'] = ['>', ['fibery/creation-date'], '$lastMaxTimestamp'];
        }

        console.log('query', JSON.stringify({params, query}, null, 2));

        const entities = await getEntitiesViaFibery(fibery, query, params);
        // const entities = await getEntitiesViaAxios(fibery, query, params);

        if (!entities.length) {
            console.log(`No new entities.`);
            return
        }

        const metadata = {typeId, lastMaxTimestamp};


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
        this.db.set("lastMaxTimestamp", maxTimestamp)
    },
}
