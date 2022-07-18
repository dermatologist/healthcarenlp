

#Change this variable to True if you want to debug the Interactive Runner Pipeline else it uses Dataflow
from apache_beam.io.gcp.internal.clients import bigquery
import google.auth
from google.cloud import bigquery
from google.auth.transport.requests import AuthorizedSession
import apache_beam as beam

from dotted_dict import DottedDict
import utils
import config

debug = config.debug
DATASET = config.DATASET
TEMP_LOCATION = config.TEMP_LOCATION

PROJECT = config.PROJECT
LOCATION = config.LOCATION
URL = config.URL
NLP_SERVICE = config.NLP_SERVICE
GCS_BUCKET = config.GCS_BUCKET
documentReference = config.documentReference


# Construct a BigQuery client object.

TABLE_ENTITY = "entities"
TABLE_REL = "relations"
TABLE_ENTITYMENTIONS = "entitymentions"

schemaEntity = [
    bigquery.SchemaField("entityId", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("preferredTerm", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("vocabularyCodes", "STRING", mode="REPEATED"),
]

schemaRelations = [
    bigquery.SchemaField("subjectId", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("objectId", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("confidence", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("id", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("patient", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("documentReference", "STRING", mode="NULLABLE"),
]

schemaEntityMentions = [
    bigquery.SchemaField("mentionId", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("type", "STRING", mode="NULLABLE"),
    bigquery.SchemaField(
        "text",
        "RECORD",
        mode="NULLABLE",
        fields=[
             bigquery.SchemaField("content", "STRING", mode="NULLABLE"),
             bigquery.SchemaField("beginOffset", "INTEGER", mode="NULLABLE"),
        ],
    ),
    bigquery.SchemaField(
        "linkedEntities",
        "RECORD",
        mode="REPEATED",
        fields=[
             bigquery.SchemaField("entityId", "STRING", mode="NULLABLE"),
        ],
    ),
    bigquery.SchemaField(
        "temporalAssessment",
        "RECORD",
        mode="NULLABLE",
        fields=[
             bigquery.SchemaField("value", "STRING", mode="NULLABLE"),
             bigquery.SchemaField("confidence", "FLOAT64", mode="NULLABLE"),
        ],
    ),
    bigquery.SchemaField(
        "certaintyAssessment",
        "RECORD",
        mode="NULLABLE",
        fields=[
             bigquery.SchemaField("value", "STRING", mode="NULLABLE"),
             bigquery.SchemaField("confidence", "FLOAT64", mode="NULLABLE"),
        ],
    ),
    bigquery.SchemaField(
        "subject",
        "RECORD",
        mode="NULLABLE",
        fields=[
             bigquery.SchemaField("value", "STRING", mode="NULLABLE"),
             bigquery.SchemaField("confidence", "FLOAT64", mode="NULLABLE"),
        ],
    ),
    bigquery.SchemaField("confidence", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("id", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("patient", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("documentReference", "STRING", mode="NULLABLE"),
]

client = bigquery.Client()

# Create Table IDs
table_ent = PROJECT+"."+DATASET+"."+TABLE_ENTITY
table_rel = PROJECT+"."+DATASET+"."+TABLE_REL
table_mentions = PROJECT+"."+DATASET+"."+TABLE_ENTITYMENTIONS

# If table exists, delete the tables.
client.delete_table(table_ent, not_found_ok=True)
client.delete_table(table_rel, not_found_ok=True)
client.delete_table(table_mentions, not_found_ok=True)

# Create tables

table = bigquery.Table(table_ent, schema=schemaEntity)
table = client.create_table(table)  # Make an API request.

print(
    "Created table {}.{}.{}".format(
        table.project, table.dataset_id, table.table_id)
)

table = bigquery.Table(table_rel, schema=schemaRelations)
table = client.create_table(table)  # Make an API request.
print(
    "Created table {}.{}.{}".format(
        table.project, table.dataset_id, table.table_id)
)
table = bigquery.Table(table_mentions, schema=schemaEntityMentions)
table = client.create_table(table)  # Make an API request.
print(
    "Created table {}.{}.{}".format(
        table.project, table.dataset_id, table.table_id)
)

with beam.Pipeline() as p:
    documents = (p | 'ReadDocument' >> utils.ReadLinesFromText(GCS_BUCKET))
    nlp_annotations = (documents | "Analyze" >> utils.AnalyzeLines())

    resultsEntities = (nlp_annotations
                    | "Break" >> beam.ParDo(utils.breakUpEntities())
                    | "WriteEntitiesToBigQuery" >> beam.io.WriteToBigQuery(
                        PROJECT+":"+DATASET+"."+TABLE_ENTITY,
                        method="STREAMING_INSERTS",
                        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                        create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER)
                    )
    resultsRelationships = (nlp_annotations
                            | "GetRelationships" >> beam.ParDo(utils.getRelationships())
                            | "WriteRelsToBigQuery" >> beam.io.WriteToBigQuery(
                                PROJECT+":"+DATASET+"."+TABLE_REL,
                                method="STREAMING_INSERTS",
                                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER)
                        )

    resultsMentions = (nlp_annotations
                            | "GetMentions" >> beam.ParDo(utils.getEntityMentions())
                            | "WriteMentionsToBigQuery" >> beam.io.WriteToBigQuery(
                                PROJECT+":"+DATASET+"."+TABLE_ENTITYMENTIONS,
                                method="STREAMING_INSERTS",
                                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER)
                        )

