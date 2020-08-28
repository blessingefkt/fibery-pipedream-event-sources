const Fibery = require('fibery-unofficial');
const sortby = require('lodash.sortby');

const FIBERY_META = {
    complexTypes: [
        "fibery/file",
        "Collaboration~Documents/Document"
    ],
    standardFields: [
        'fibery/id',
        'fibery/public-id',
        'fibery/creation-date',
        'fibery/url',
        'fibery/type',
    ],
    hiddenTypes: [
        "Collaboration~Documents/Reference"
    ],
    invalidFieldNames: [
        "fibery/id",
        "fibery/rank",
        "fibery/public-id"
    ],
    validFieldNames: [
        "workflow/state",
        "Files/Files"
    ]
};
const fibery = {
    getHost() {
        return this._host || process.env.FIBERY_HOST;
    },
    getToken() {
        return this._token || process.env.FIBERY_TOKEN;
    },
    init(instance) {
        if (instance) {
            this._host = instance.host;
            this._token = instance.token;
        }
        if (!this.getHost() || !this.getToken()) {
            throw new Error('Please configure the following environment variables: FIBERY_HOST, FIBERY_TOKEN');
        }
        this._client = this.getClient();
        return this;
    },
    getClient() {
        return new Fibery({
            host: String(this.getHost()).trim(),
            token: String(this.getToken()).trim()
        });
    },
    getDefaultFieldNames(fields) {
        return (fields || [])
            .filter(f => f['fibery/type'] === 'fibery/url'
                || (f['fibery/meta'] && f['fibery/meta']['ui/title?'])
                || FIBERY_META.standardFields.includes(f['fibery/name'])
            )
            .map(f => f['fibery/name'])
    },
    getSimpleFields(fields) {
        return (fields || [])
            .filter(f => !FIBERY_META.hiddenTypes.includes(f['fibery/type'])
                && !FIBERY_META.invalidFieldNames.includes(f['fibery/name'])
                && (
                    FIBERY_META.validFieldNames.includes(f['fibery/name'])
                    || f['fibery/name'].startsWith('fibery/')
                    || f['fibery/meta']['fibery/collection?']
                    || f['fibery/meta']['fibery/relation']
                    || FIBERY_META.complexTypes.includes(f['fibery/type'])
                ));
    },
    getRelatedFields(fields) {
        return (fields || [])
            .filter(f => !f['fibery/meta']['fibery/relation']
                && !FIBERY_META.standardFields.includes(f['fibery/name'])
                && (
                    f['fibery/name'].startsWith('fibery/')
                    || FIBERY_META.complexTypes.includes(f['fibery/type'])
                ));
    },
    async schema() {
        if (!this._client) {
            console.warn('client not set');
            return [];
        }
        if (!this._schema)
            this._schema = await this._client.getSchema();
        return this._schema;
    },
    async typeOptions() {
        const schema = await this.schema();
        return uniqueArray(
            schema
                .filter(type => !FIBERY_META.hiddenTypes.includes(type['fibery/name']))
                .map(type => type['fibery/name'])
        ).map(fieldName => ({value: fieldName, label: fieldName,}));
    },
    async getType(typeId) {
        const schema = await this.schema();
        return schema.find(type => type['fibery/name'] === typeId || type['fibery/id'] === typeId);
    },
    async getTypeFields(typeId) {
        const type = await this.getType(typeId);
        if (!type) {
            console.error('Type not found', typeId);
        }
        return type ? type['fibery/fields'] : [];
    },
    async getTypeFieldOptions(typeId) {
        const fields = await this.getTypeFields(typeId);
        const allFields = [...this.getSimpleFields(fields), ...this.getRelatedFields(fields)];
        const uniqueFieldNames = uniqueArray(allFields.map(f => f['fibery/name']));
        return uniqueFieldNames
            .map(fieldName => allFields.find(f => f['fibery/name'] === fieldName))
            .map(f => ({value: f['fibery/id'], label: f['fibery/name']}));
    },
    async queryEntities(query, params) {
        return await this._client.entity.query(query, params);
    }
};
module.exports = {
    name: "New Fibery Entities Created",
    version: "0.0.1",
    props: {
        db: "$.service.db",
        host: "string",
        token: "string",
        typeId: {
            type: "string",
            async options() {
                return sortby(await fibery.init(this).typeOptions(), ['label']);
            }
        },
        fields: {
            type: "string[]",
            async options() {
                return sortby(await fibery.init(this).getTypeFieldOptions(this.typeId), ['label']);
            },
            default: []
        },
        timer: {
            type: "$.interface.timer",
            default: {
                intervalSeconds: 60 * 5,
            },
        },
    },
    async run(event) {
        const {typeId, fields} = this;
        console.log('props', {typeId, fields});
        const params = {};
        const orderBy = [
            [['fibery/creation-date'], 'q/asc']
        ];

        fibery.init(this);

        const type = await fibery.getType(typeId);
        const fieldNames = fields
            .map(value => {
                if (String(value).includes('/'))
                    return value;
                if (typeof value === 'object')
                    return value.label;
                const field = type['fibery/fields'].find(f => f['fibery/id'] === value);
                return field ? field['fibery/name'] : null;
            })
            .filter(value => !!value);

        console.log('fieldNames', fieldNames);

        const selectedFields = await appendSelects(type, fieldNames);

        console.log('selectedFields', selectedFields);

        const query = {
            'q/from': typeId,
            'q/where': undefined,
            'q/order-by': orderBy,
            'q/select': Array.from(selectedFields),
            'q/limit': 3,
        };

        let maxTimestamp
        const dbKey = `lastMaxTimestamp/${fibery.getHost()}/${typeId}`;
        // '2020-01-27T22:54:51.250Z' ||
        const lastMaxTimestamp = this.db.get(dbKey);
        if (lastMaxTimestamp) {
            params['$lastMaxTimestamp'] = lastMaxTimestamp;
            query['q/where'] = ['>', ['fibery/creation-date'], '$lastMaxTimestamp'];
        }

        console.log('query', JSON.stringify({params, query}, null, 2));
        let entities = [];

        entities = await fibery.queryEntities(query, params);
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
        this.db.set(dbKey, maxTimestamp)
    },
}
const uniqueArray = (values) => Array.from(new Set(values));

async function appendSelects(type, fieldNames) {
    const typeFields = type['fibery/fields'];
    const selects = fibery.getDefaultFieldNames(typeFields);
    await Promise.all(typeFields
        .filter(fibField => fieldNames.includes(fibField['fibery/name'])
            || FIBERY_META.standardFields.includes(fibField['fibery/name']))
        .map(async fibField => {
            const fieldName = fibField['fibery/name'];
            const fieldTypeName = fibField['fibery/type'];
            const fieldType = await fibery.getType(fieldTypeName);

            const typeMeta = fieldType['fibery/meta'] || {};
            const fieldMeta = fibField['fibery/meta'] || {};

            // console.debug('FIELD', fieldName, fibField['fibery/id'], fibField);

            const isComplexType = FIBERY_META.complexTypes.some(t => t === fieldTypeName || fieldTypeName.startsWith(t));
            const isEnum = typeMeta['fibery/enum?'];
            const isFileType = fieldTypeName === 'fibery/file';
            const isPrimitive = typeMeta['fibery/primitive?'];
            const isRelation = fieldMeta['fibery/relation'];
            const isCollection = fieldMeta['fibery/collection?'];

            // console.debug('FIELD', fieldName, {
            //     isPrimitive,
            //     isComplexType,
            //     isRelation,
            //     isCollection,
            //     isEnum,
            //     typeMeta,
            //     fieldMeta
            // });

            if (isPrimitive) {
                selects.push(fieldName);
            } else if (!fieldTypeName.endsWith('/Reference')) {
                const fields = [];
                if (isFileType) {
                    fields.push(...fieldType['fibery/fields'].map(f => f['fibery/name']));
                }
                if (isEnum) {
                    fields.push(...fieldType['fibery/fields'].map(f => f['fibery/name']).filter(f => f.startsWith('enum/')))
                }
                if (isRelation) {
                    fields.push(...fibery.getDefaultFieldNames(fieldType['fibery/fields']));
                }
                if (isComplexType) {
                    fields.push(fieldTypeName.split('/', 2)[0] + "/secret")
                }

                if (fields.length > 0)
                    selects.push({
                        [fieldName]: isCollection ? {
                            "q/select": uniqueArray(fields),
                            "q/limit": 'q/no-limit',
                        } : uniqueArray(fields)
                    })
            }
        }));
    return uniqueArray(selects);
}
