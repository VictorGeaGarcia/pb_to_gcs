# pb_to_gcs

## PREVIOUS CONSIDERATIONS

Using latest versions of Apache-Beam (2.10 - 2.13) throws an error like 

```pyhton
TypeError: Cannot convert GlobalWindow to apache_beam.utils.windowed_value._IntervalWindowBase ...
```
Some SO posts dealing with this: [1](https://stackoverflow.com/questions/54745869/how-to-create-a-dataflow-pipeline-from-pub-sub-to-gcs-in-python), [2](https://stackoverflow.com/questions/55109403/apache-beam-python-sdk-upgrade-issue).
Official JIRA issue [3] (https://issues.apache.org/jira/browse/BEAM-6860)

Therefore we need to use beam 2.9.0:

```python 
pip install apache-beam[gcp]===2.9.0
```

However when runnining the previous command, it returns an error like:
```
  ERROR: Could not find a version that satisfies the requirement apache-beam===2.9.0 (from versions: 0.6.0, 2.0.0, 2.1.0, 2.1.1, 2.2.0, 2.11.0, 2.12.0, 2.13.0)
ERROR: No matching distribution found for apache-beam===2.9.0
```
This shouldn't be a big issue to solve, but for now I'll use Python2 since also [Python3 is still not fully supported](https://jira.apache.org/jira/browse/BEAM-1251) and there are addititional issues/bugs.

## THEREFORE OUR SETUP WILL CONSIST OF:

Python2 with Apache Beam 2.9.0:

1. Create virtual environment and install apache-beam===2.9.0"

```python
virtualenv py2_beam_env && source py2_beam_env/bin/activate && pip install apache-beam[gcp]===2.9.0
```

(If your default python is not python2 then do add some flag like: -p python2 in the previous command).

-------------------------------

## PubSub to GCS in streaming:

Using code in `pb_to_gcs_with_splits.py`. Change `default_topic` and `default_bucket` parameters to the actual desired values:

default_topic = 'projects/\<your-project-id\>/topics/\<your-topic\>'                                                                                                                                                                            
default_bucket = 'gs://\<gs-bucket-name\>/data-team/sync/'  

The pipeline should be run with command (changing project and temp_location flags to your appropriate values):

```python
python pb_to_gcs_diffoutputs.py --streaming --project <your-project-id> --temp_location gs://<your-gcs-bucket>/temp --runner DataflowRunner --experiments=allow_non_updatable_job
```

The previous file has windows of 1 minute ("WINDOW_LENGTH = 60\*1")

Pipeline starts reading from PubSub using ("beam.io.ReadFromPubSub(topic=default-topic)") using the designated topic.

It splits the pipeline to generate two different outputs depending on if pubsub message is "cs_bookings" or "users" (simply checking if "vehicle_id" is in the message (if it is then it should go to "cs_bookings". Obviously higher control can be added). We make use of a custom Class DiffOutputsFn(beam.DoFn). Similar implementation [than in example here]("https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/cookbook/multiple_output_pardo.py")

When running the pipeline, if we publish something like:

```bash
gcloud pubsub topics publish ub-poc-topic  --message '{"op": "create","timestamp": 1562229006,"source": "cs_bookings","payload": {"id": 1,"user_id": 24,"vehicle_id": 65,"location_id": 4,"start": 1562229000,"end": 1562229005,"real_end": 1562229004,"status": "whateverstatus"}}'
```

will send the message to `cs_bookings` folder.

Something like (without `vehicle_id`): 

'{"op": "create","timestamp": 1562229006,"source": "users","payload": {"id": 1,"user_id": 24,"location_id": 4,"start": 1562229000,"end": 1562229005,"real_end": 1562229004,"status": "whateverstatus"}}'

will send the message to `users` folder.


#TODO: Right now it doesn't add the proper datetime to the file, and it actually creates an additional folder which makes everything more noisy. It should be possible to modify this with something like the code snippet below. But I didn't have time to try this.

```python

class WriteToSeparateFiles(beam.DoFn):
    def __init__(self, outdir):
        self.outdir = outdir
    def process(self, element):
        writer = filesystems.FileSystems.create(self.outdir + element['some_key'] + '.json')
        writer.write(element['image'])
        writer.close()
```

## ADDITIONAL CONSIDERATIONS:

Command for running pipeline has additional parameter --experiments.... because if you don't add it returns the error:

#TODO: Add error
```

Workflow failed. Causes: Because of the shape of your pipeline, the Cloud Dataflow job optimizer produced a job graph that is not updatable using the --update pipeline option. This is a known issue that we are working to resolve. See https://issuetracker.google.com/issues/118375066 for information about how to modify the shape of your pipeline to avoid this error. You can override this error and force the submission of the job by specifying the --experiments=allow_non_updatable_job parameter., The stateful transform named 'WriteGCSCommon_cs_bookings/Write/WriteImpl/DoOnce/Decode Values.out/FromValue/ReadStream' is in two or more computations.
```
Adding this parameter make the pipeline not being updatable (anyway currently [Python streaming pipeines are not updatable](https://beam.apache.org/documentation/sdks/python-streaming/#dataflowrunner-specific-features)

