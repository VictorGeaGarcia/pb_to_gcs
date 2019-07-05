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
pip install apache-beam===2.9.0
```

However when runnining the previous command, it returns an error like:
```
  ERROR: Could not find a version that satisfies the requirement apache-beam===2.9.0 (from versions: 0.6.0, 2.0.0, 2.1.0, 2.1.1, 2.2.0, 2.11.0, 2.12.0, 2.13.0)
ERROR: No matching distribution found for apache-beam===2.9.0
```
This shouldn't be a big issue to solve, but for now I'll use Python2 since also [Python3 is still not fully supported] (https://jira.apache.org/jira/browse/BEAM-1251) and there are addititional issues/bugs.

## THEREFORE OUR SETUP WILL CONSIST OF:

Python2 with Apache Beam 2.9.0:

1. Create virtual environment and install apache-beam===2.9.0"

```python
virtualenv py2_beam_env && source py2_beam_env/bin/activate && pip install apache-beam===2.9.0
```

-------------------------------

## FULL SOLUTION (With one issue):

Using code in `pb_to_gcs_with_splits.py`. Change `default_topic` and `default_bucket` parameters to the actual desired values:

default_topic = 'projects/<your-project-id>/topics/<your-topic>'                                                                                                                                                                            
default_bucket = 'gs://<gs-bucket-name>/data-team/sync/cs_bookings/'  #TODO: change output name depending on cs_bookings or users 


The pipeline should be run with command:

#TODO: add full command

The previous file has windows of 1 minute ("WINDOW_LENGTH = 60\*1")

Pipeline starts reading from PubSub using ("beam.io.ReadFromPubSub(topic=default-topic)") using the designated topic.

It splits the pipeline two generate two different outputs depending on if pubsub message is "cs_bookings" or "users", by making use of Class DiffOutputsFn(beam.DoFn). Similar implementation [than in example here](#TODO: Add example link)

When running the pipeline this returns an error related to the `pvalue` method imported in the `DiffOutputsFn`. Solution should be to create a setup.py adding [the class as a different module](https://stackoverflow.com/questions/52874383/gcp-dataflow-apache-beam-problem-import-another-python-file-to-main-py-with-co?rq=1). Similar to [what I have in this other repo](https://github.com/VictorGeaGarcia/Apache-Beam/tree/master/Dataflow_Using_Python_Dependencies)


## BASIC PIPELINE NOT SPLITTING THE OUTPUTS:

This pipeline is more straightforward, not splliting the outputs:

#TODO: Add command to run it

#TODO: Right now it doesn't add the proper datetime to the file. There is not a builtin implementation for this in Python SDK - WriteIO module. Maybe it's possible to add a custom implementation, but I'm not sure how feasible it is right now.


## ADDITIONAL CONSIDERATIONS:

Command for running pipeline has additional parameter --experiments.... because if you don't add it returns the error:

#TODO: Add error
```ERROR
```
Adding this parameter make the pipeline not being updatable (anyway currently [Python streaming pipeines are not updatable](https://beam.apache.org/documentation/sdks/python-streaming/#dataflowrunner-specific-features)


## OTHER OPTIONS:

Another option would be to use PubSub --> BigQuery directly. Althought this would be writing in Streaming, and therefore incurring in [BigQuery streaming inserts costs](#TODO: add link)

To get a full implementation of all features (using custom names in GCS...), right now it would be needed to use Java SDK with some pipeline like
