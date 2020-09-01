const helpers = require('https://github.com/blessingefkt/fibery-pipedream-event-sources/fibery.helpers.js');

/**
 * @param {{api_key:String, account_name?:string, host?:string}} credentials
 */
module.exports = function FiberyAdapter(credentials) {
    const sortby = require('lodash.sortby');
    const Fibery = require('fibery-unofficial');
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
        async schema() {
            if (!this._schema)
                this._schema = helpers.getSchemaMap(await this.getClient().getSchema());
            return this._schema;
        },
        async typeOptions() {
            const schema = await this.schema();
            return sortby(
                helpers.uniqueArray(
                    schema
                        .filter(type => !helpers.isHiddenType(type['fibery/name']))
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
            const allFields = [...helpers.getSimpleFields(fields), ...helpers.getRelatedFields(fields)];
            return sortby(
                helpers.uniqueArray(allFields.map(f => f['fibery/name']))
                    .map(fieldName => allFields.find(f => f['fibery/name'] === fieldName))
                    .map(f => ({value: f['fibery/id'], label: f['fibery/name']})),
                ['label']
            );
        },
        async queryEntities(query, params) {
            return await this.getClient().entity.query(query, params);
        },
        async getQueryObject(typeNameOrId, {fields, dateFields, limit, lastMaxTimestamp}) {
            const schemaMap = await this.getSchema();
            return helpers.getQueryObject(schemaMap, typeNameOrId, {fields, dateFields, limit, lastMaxTimestamp});
        }
    }
}
