INSERT OVERWRITE TABLE ${dbName}.solr_lst_updated_dt
SELECT unix_timestamp('${resetTime}'), unix_timestamp('${resetTime}'), unix_timestamp('${resetTime}'), unix_timestamp('${resetTime}'), unix_timestamp('${resetTime}'), unix_timestamp('${resetTime}') FROM ${dbName}.solr_lst_updated_dt;
