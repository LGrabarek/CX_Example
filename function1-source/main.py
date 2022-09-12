import json
from google.cloud import aiplatform


project_id = "curious-skyline-360213"
dataset_id = "CX"
pipeline_root_path = "gs://curious-skyline/Projects/CX/CXpipeline"

def process_request(request):
   """Processes the incoming HTTP request.

   Args:
     request (flask.Request): HTTP request object.

   Returns:
     The response text or any set of values that can be turned into a Response
     object using `make_response
     <http://flask.pocoo.org/docs/1.0/api/#flask.Flask.make_response>`.
   """

   # decode http request payload and translate into JSON object
   request_str = request.data.decode('utf-8')
   request_json = json.loads(request_str)

   pipeline_spec_uri = request_json['pipeline_spec_uri']
   parameter_values = request_json['parameter_values']

   aiplatform.init(
       project=project_id,
       staging_bucket=pipeline_root_path
       )

   job = aiplatform.PipelineJob(
    display_name='cx-pipeline-fake-data',
    template_path=pipeline_spec_uri,
    pipeline_root=pipeline_root_path,
    enable_caching=False,
    parameter_values=parameter_values
       )

   job.submit()