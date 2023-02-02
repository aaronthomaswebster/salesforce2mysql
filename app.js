const jsforce = require("jsforce");
const mysql = require("mysql2");
const csv = require("csv-parser");
const path = require("path");
const fs = require("fs");
const { v4: uuidv4 } = require("uuid");
require("dotenv").config();

const sfDir = "./sfData";

const username = process.env.SF_USERNAME;
const password = process.env.SF_PASSWORD;

const conn = new jsforce.Connection({
    loginUrl: process.env.SF_LOGIN_URL,
});
conn.bulk.pollInterval = 5000; // 5 sec
conn.bulk.pollTimeout = 600000; // 60 sec


const db = mysql.createConnection({
    host: process.env.DB_HOST,
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_NAME
});

const tablesToSkip = [
    "IdeaComment",
    "ContentDocumentLink",
    "ContentVersion",
    "ChatterActivity",
    "ActiveScratchOrg",
    "DuplicateRecordItem",
    "LiveChatTranscriptEvent",
];
const tablesToInclude = [
    "Account",
    "Contact",
    "Opportunity",
    "RecordType",
    "User",
];

function login() {
    return new Promise((resolve, reject) => {
        conn.login(username, password, (err, res) => {
            if (err) {
                console.log("Error connecting to Salesforce");
                reject(err);
            }
            console.log("Connected to Salesforce");
            resolve(res);
        });
    });
}

function getTableNames() {
    let tableNames = [];
    return new Promise((resolve, reject) => {
        conn.describeGlobal(function (err, res) {
            if (err) {
                return console.error(err);
            }
            res.sobjects.forEach((sobject) => {
                if (tablesToInclude.includes(sobject.name))
                    tableNames.push(sobject.name);

                // if (sobject.name == 'RecordType')
                //     tableNames.push(sobject.name);
                // if (!sobject.deprecatedAndHidden && sobject.queryable && sobject.retrieveable && sobject.triggerable && !tablesToSkip.includes(sobject.name))
                //     tableNames.push(sobject.name);
            });
            resolve(tableNames);
        });
    });
}

function getTableFields(tableName) {
    console.log("getting fields for " + tableName + "...");
    return new Promise((resolve, reject) => {
        conn.sobject(tableName).describe(function (err, meta) {
            if (err) {
                console.error(err);
                reject();
            }
            console.log("Retrieved metadata for " + tableName + " object:");
            const fields = sfToSql(meta.fields, tableName);
            resolve(fields);
        });
    });
}

function sfToSql(fields, tableName) {
    let fieldSet = [];
    fields.forEach((field) => {
        let fieldData = {
            name: field.name,
        };
        switch (field.type) {
            case "id":
                fieldData.type = "VARCHAR(18)";
                fieldData.nullable = false;
                fieldData.foreignKey = false;
                break;
            case "string":
            case "url":
            case "textarea":
            case "picklist":
            case "multipicklist":
            case "email":
            case "phone":
            case "combobox":
                if (field.length > 200) {
                    fieldData.type = "TEXT";
                } else {
                    fieldData.type = `VARCHAR(${field.length})`;
                }
                if (field.type == 'picklist') {
                    fieldData.type = `VARCHAR(60)`;
                }
                fieldData.nullable = true;
                fieldData.foreignKey = false;
                break;
            case "reference":
                if (field.polymorphicForeignKey) {
                    fieldData.type = `VARCHAR(${field.length})`;
                    fieldData.nullable = true;
                    fieldData.foreignKey = false;
                    fieldData.lookupTo = field.referenceTo;
                } else {
                    fieldData.type = `VARCHAR(${field.length})`;
                    fieldData.nullable = true;
                    fieldData.foreignKey = true;
                    fieldData.lookupTo = field.referenceTo[0];
                    fieldData.relationshipName = field.relationshipName;
                }
                break;
            case "int":
                fieldData.type = "INT";
                fieldData.nullable = field.nillable;
                fieldData.foreignKey = false;
                break;
            case "datetime":
                fieldData.type = "DATETIME";
                fieldData.nullable = field.nillable;
                fieldData.foreignKey = false;
                break;
            case "date":
                fieldData.type = "DATE";
                fieldData.nullable = field.nillable;
                fieldData.foreignKey = false;
                break;
            case "time":
                fieldData.type = "TIME";
                fieldData.nullable = field.nillable;
                fieldData.foreignKey = false;
            case "boolean":
                fieldData.type = "varchar(5)";
                fieldData.nullable = field.nillable;
                fieldData.foreignKey = false;
                break;
            case "double":
            case "currency":
            case "percent":
                fieldData.type = `DECIMAL(${field.precision},${field.scale})`;
                fieldData.nullable = field.nillable;
                fieldData.foreignKey = false;
                break;
            case "base64":
                fieldData.type = `TEXT`;
                fieldData.nullable = field.nillable;
                fieldData.foreignKey = false;
                break;
            case "address":
                fieldData.type = `TEXT`;
                fieldData.nullable = field.nillable;
                fieldData.foreignKey = false;
                break;
            case "location":
            case "encryptedstring":
                break;
            default:
                console.log(field);
                cat == dog;
            // code block
        }
        if (
            !["address", "location", "encryptedstring"].includes(field.type) &&
            field.name != undefined
        ) {
            fieldSet.push(fieldData);
        }
    });
    return fieldSet;
}

function getMetadata() {
    return new Promise(async (resolve, reject) => {
        var tables = [];
        let tableNames = await getTableNames();
        for (let i = 0; i < tableNames.length; i++) {
            let fields = await getTableFields(tableNames[i]);
            tables.push({
                name: tableNames[i],
                fields: fields,
            });
        }
        resolve(tables);
    });
}

function executeSQLQuery(query) {
    return new Promise((resolve, reject) => {
        db.query(query, (err, res) => {
            if (err) {
                console.error(err);
                reject();
            }
            resolve();
        });
    });
}

async function bulkSFQuery(query, table) {
    console.log("querying " + table);
    console.log(query);
    return new Promise((resolve, reject) => {
        let fileStream = fs.createWriteStream(`${sfDir}/${table}.csv`);
        let bulkQuery = conn.bulk.query(query);
        bulkQuery.on("end", function () {
            console.log("End of data stream");
        });
        bulkQuery.on("error", function (err) {
            console.log("Error occurred during bulk query:", err);
            reject(err);
        });
        bulkQuery.on("finish", function () {
            console.log("Finished writing data to file");
        });
        bulkQuery.on("readable", function () {
            console.log("Data is now available to be read from stream");
        });
        bulkQuery.on("drain", function () {
            console.log("Write buffer is now empty");
        });
        bulkQuery.on("close", function () {
            console.log("Stream and its underlying resources have been closed");
        });
        bulkQuery.on("pipe", function () {
            console.log("Readable stream has been piped to writable stream");
        });
        bulkQuery.stream().pipe(fileStream);
        fileStream.on("finish", function () {
            console.log("Done querying " + table);
            resolve();
        });
        fileStream.on("error", function (err) {
            console.log(err);
            console.log("Error writing data to file");
            reject(err);
        });
        fileStream.on("end", function () {
            console.log("End of file stream");
        });
    });
}


function runSQlInsert() {
    return new Promise(async (resolve) => {
        files = await new Promise((resolve, reject) => {
            console.log("Reading files...")
            fs.readdir('./sfData', (err, files) => {
                if (err) reject(err);
                resolve(files);
            });
        });
        console.log("Files read successfully")
        console.log('Files: ', files.length);
        console.log("Importing data...")
        for (let file of files) {
            await importData(file);
        }
        resolve();
    });
}




function convertDate(date) {
    const dateRegex = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/;
    if (!dateRegex.test(date)) {
        return date;
    }
    const newDate = new Date(date);
    let dateString = newDate.getFullYear() + "-" + (newDate.getMonth() + 1) + "-" + newDate.getDate();
    let timeString = newDate.getHours() + ":" + newDate.getMinutes() + ":" + newDate.getSeconds();
    return dateString + " " + timeString;
}

async function insertIntoSQL(data, file) {
    const tableName = path.parse(file).name;
    columns = Object.keys(data);
    data = Object.values(data).map(datum => datum === '' ? null : convertDate(datum));
    await new Promise((resolve, reject) => {
        db.query(`INSERT INTO \`${tableName}\` (\`${columns.join('\`, \`')}\`) VALUES (?)`, [data], function (error, results, fields) {
            if (error) reject(error);
            resolve();
        });
    });
}

function setForeignKeyCheck(value) {
    console.log(`Setting foreign key check to ${value}...`);
    return new Promise((resolve, reject) => {
        db.query(`SET FOREIGN_KEY_CHECKS = ${value};`, (err, res) => {
            if (err) {
                console.error(err);
                reject();
            }
            resolve();
        });
    });
}




function importData(file) {
    console.log(`Importing data from ${file}...`)
    var parser = csv({ delimiter: ',', columns: true, trim: true });
    return new Promise(async (resolve, reject) => {
        fs.createReadStream(`./sfData/${file}`)
            .pipe(parser)
            .on('data', async function (data) {
                //pause the stream
                parser.pause();
                await insertIntoSQL(data, file);
                //resume the stream
                parser.resume();
            })
            .on('end', function () {
                console.log(`Data from ${file} imported successfully`);
                resolve();
            });
    });
}











async function run() {
    await login();
    await setForeignKeyCheck(0);
    //await insertIntoSQL();
    let tableNames = [];
    const metadata = await getMetadata();
    console.log("creating tables");
    await Promise.all(
        metadata.map(async (meta) => {
            //create tables and non-foreign key columns
            let nonForeignKeyColumns = meta.fields.filter((field) => {
                return !field.foreignKey;
            });
            let columns = nonForeignKeyColumns.map((field) => {
                return `\`${field.name}\` ${field.type} ${field.nullable ? "NULL" : "NOT NULL"
                    } ${field.name === "Id" ? "PRIMARY KEY" : ""}`;
            });
            let query = `DROP TABLE IF EXISTS \`${meta.name}\` ;`;
            await executeSQLQuery(query);
            query = ` CREATE TABLE IF NOT EXISTS \`${meta.name}\`  (${columns.join(
                ", "
            )})  ENGINE=MYISAM ROW_FORMAT=DYNAMIC;`;

            await executeSQLQuery(query);
            console.log("created table " + meta.name);
            tableNames.push(meta.name);
        })
    );
    console.log("creating foreign keys");
    await Promise.all(
        metadata.map(async (meta) => {
            let foreignKeys = meta.fields.filter((field) => {
                return field.foreignKey;
            });
            if (foreignKeys.length === 0) return;
            let columns = foreignKeys.map((field) => {
                return `\`${field.name}\` ${field.type} ${field.nullable ? "NULL" : "NOT NULL"
                    }`;
            });
            //create additional foreign key columns
            const query = `ALTER TABLE \`${meta.name}\` ADD COLUMN (${columns.join(
                ", "
            )});`;
            await executeSQLQuery(query);
            console.log("created foreign keys for " + meta.name);
            console.log("creating foreign key constraints for " + meta.name);
            await Promise.all(
                foreignKeys.map(async (field) => {
                    if (tableNames.indexOf(field.lookupTo) === -1) {
                        return;
                    }
                    let query = `ALTER TABLE \`${meta.name
                        }\` ADD CONSTRAINT \`${uuidv4().replaceAll(
                            "-",
                            ""
                        )}_fk\` FOREIGN KEY (\`${field.name}\`) REFERENCES \`${field.lookupTo
                        }\` (Id);`;
                    await executeSQLQuery(query);
                })
            );
            console.log("created foreign key constraints for " + meta.name);
        })
    );
    console.log("getting data");
    for (let i = 0; i < metadata.length; i++) {
        let meta = metadata[i];
        await bulkSFQuery(
            `SELECT ${meta.fields.map((field) => field.name).join(", ")} FROM ${meta.name
            }`,
            meta.name
        );
    }
    await runSQlInsert();
    await setForeignKeyCheck(1);
    db.end();
}

run();
