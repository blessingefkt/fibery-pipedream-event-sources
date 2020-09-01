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

const uniqueArray = (values) => Array.from(new Set(values));

function getDefaultFieldNames(fields) {
    return (fields || [])
        .filter(f => f['fibery/type'] === 'fibery/url'
            || (f['fibery/meta'] && f['fibery/meta']['ui/title?'])
            || FIBERY_META.standardFields.includes(f['fibery/name'])
        )
        .map(f => f['fibery/name'])
}

function appendSelects(schema, typeNameOrId, fieldNames) {
    const typeFields = schema[typeNameOrId]['fibery/fields'];
    const selects = getDefaultFieldNames(typeFields);
    typeFields
        .filter(fibField => fieldNames.includes(fibField['fibery/name'])
            || FIBERY_META.standardFields.includes(fibField['fibery/name']))
        .map(fibField => {
            const fieldName = fibField['fibery/name'];
            const fieldTypeName = fibField['fibery/type'];
            const fieldType = schema[fieldTypeName];

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
                    fields.push(...getDefaultFieldNames(fieldType['fibery/fields']));
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
        });
    return uniqueArray(selects);
}

function getQueryObject(schema, typeNameOrId, {fields, dateFields, limit, lastMaxTimestamp}) {
    const type = schema[typeNameOrId];
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

    const selectedFields = appendSelects(schema, type, fieldNames);
    const queryObject = {
        query: {
            'q/from': type['fibery/name'],
            'q/select': Array.from(selectedFields),
            'q/limit': limit > 0 ? limit : 'q/no-limit'
        },
        params: {},
    };
    if (dateFields && lastMaxTimestamp) {
        queryObject.params['$lastMaxTimestamp'] = lastMaxTimestamp;
        queryObject.query['q/order-by'] = dateFields.map(dateField => [[dateField], 'q/asc']);
        if (dateFields.length > 1)
            queryObject.query['q/where'] = ['q/or']
                .concat(dateFields.map(dateField => ['>', [dateField], '$lastMaxTimestamp']));
        else
            queryObject.query['q/where'] = ['>', dateFields, '$lastMaxTimestamp'];
    }
    return queryObject;
}

/**
 * @param {{api_key:String, account_name?:string, host?:string}} credentials
 */
function FiberyAdapter(credentials) {
    return {
        getClient() {
            const {account_name, api_key, host} = credentials;
            if ((!account_name && !host) || !api_key) {
                throw new Error('Invalid auth object.');
            }
            return new Fibery({
                host: host || account_name.trim() + '.fibery.io',
                token: api_key.trim()
            });
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
            if (!this._schema)
                this._schema = await this.getClient().getSchema();
            return this._schema;
        },
        async typeOptions() {
            const schema = await this.schema();
            return sortby(
                uniqueArray(
                    schema
                        .filter(type => !FIBERY_META.hiddenTypes.includes(type['fibery/name']))
                        .map(type => type['fibery/name'])
                )
                    .map(fieldName => ({value: fieldName, label: fieldName,})),
                ['label']
            );
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
            return sortby(
                uniqueArray(allFields.map(f => f['fibery/name']))
                    .map(fieldName => allFields.find(f => f['fibery/name'] === fieldName))
                    .map(f => ({value: f['fibery/id'], label: f['fibery/name']})),
                ['label']
            );
        },
        async queryEntities(query, params) {
            return await this.getClient().entity.query(query, params);
        },
        async getQueryObject(typeNameOrId, {fields, dateFields, limit, lastMaxTimestamp}) {
            return getQueryObject(await this.getSchema(), typeNameOrId, {fields, dateFields, limit, lastMaxTimestamp});
        }
    }
}

module.exports = {
    FiberyAdapter: FiberyAdapter,
    appendSelects: appendSelects,
    getQueryObject: getQueryObject
};
