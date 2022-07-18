debug = True
DATASET = ""
TEMP_LOCATION = ""

PROJECT = ''
LOCATION = ''
URL = 'https://healthcare.googleapis.com/v1beta1/projects/' + \
    PROJECT + '/locations/'+LOCATION+'/services/nlp:analyzeEntities'
NLP_SERVICE = 'projects/'+PROJECT+'/locations/'+LOCATION+'/services/nlp'
GCS_BUCKET = ""

# FHIR resource
documentReference = {

}
