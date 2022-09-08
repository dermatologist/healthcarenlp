from apache_beam.options.pipeline_options import PipelineOptions
from sys import argv
from apache_beam import pvalue
import json
import apache_beam as beam
from apache_beam.runners.interactive.interactive_runner import InteractiveRunner
import apache_beam.runners.interactive.interactive_beam as ib
from dotted_dict import DottedDict
import google.auth
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


#Reference https://cloud.google.com/dataflow/docs/guides/specifying-exec-params#python_1
if debug:
    runnertype = "InteractiveRunner"
else:
    runnertype = "DataflowRunner"

options = PipelineOptions(
    flags=argv,
    runner=runnertype,
    project=PROJECT,
    job_name='MyHealthcareNLPJob',
    temp_location=TEMP_LOCATION,
    region=LOCATION)
class ReadLinesFromText(beam.PTransform):

    def __init__(self, file_pattern):
        self._file_pattern = file_pattern

    def expand(self, pcoll):
        return (pcoll.pipeline
                | beam.io.ReadFromText(self._file_pattern))


class InvokeNLP(beam.DoFn):

    def process(self, element):
        element = DottedDict(documentReference)  # TODO Remove this
        credentials, project = google.auth.default()
        from google.auth.transport.requests import AuthorizedSession
        import uuid
        authed_session = AuthorizedSession(credentials)
        url = URL
        payload = {
            'nlp_service': NLP_SERVICE,
            'document_content': element.content[0].attachment.data,
        }
        resp = authed_session.post(url, data=payload)
        response = resp.json()
        response['id'] = uuid.uuid4().hex[:8]
        response['documentReference'] = element.id
        response['patient'] = element.subject.reference
        yield response


class InvokeNLPBatch(beam.DoFn):

    def process(self, element):
        # element = DottedDict(documentReference)  # TODO Remove this
        credentials, project = google.auth.default()
        from google.auth.transport.requests import AuthorizedSession
        import uuid
        authed_session = AuthorizedSession(credentials)
        url = URL
        payload = {
            'nlp_service': NLP_SERVICE,
            'document_content': element.data,
        }
        resp = authed_session.post(url, data=payload)
        response = resp.json()
        response['id'] = uuid.uuid4().hex[:8]
        response['documentReference'] = element.id
        response['patient'] = element.subject.reference
        yield response

class AnalyzeLines(beam.PTransform):
    def expand(self, pcoll):
        return (
            pcoll
            | "Invoke NLP API" >> beam.ParDo(InvokeNLP())
        )


class AnalyzeLinesBatch(beam.PTransform):
    def expand(self, pcoll):
        return (
            pcoll
            | "Invoke NLP API" >> beam.ParDo(InvokeNLPBatch())
        )

class getEntityMentions(beam.DoFn):
    def process(self, element):
        obj = {}
        for e in element['entityMentions']:
            e['id'] = element['id']
            e['patient'] = element['patient']
            e['documentReference'] = element['documentReference']
            yield e


class getRelationships(beam.DoFn):
    def process(self, element):
        obj = {}
        id = element['id']
        for e in element['relationships']:
            obj = e
            obj['id'] = id
            obj['patient'] = element['patient']
            obj['documentReference'] = element['documentReference']
            yield obj


class breakUpEntities(beam.DoFn):
    def process(self, element):
        for e in element['entities']:
            print(e)
            yield e
