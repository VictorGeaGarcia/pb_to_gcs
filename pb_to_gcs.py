import argparse                                                                                                                                                                                                                                 
import logging                                                                                                                                                                                                                                  
                                                                                                                                                                                                                                                
import apache_beam as beam                                                                                                                                                                                                                      
from apache_beam.options.pipeline_options import PipelineOptions                                                                                                                                                                                
from apache_beam.io.gcp.pubsub import ReadFromPubSub                                                                                                                                                                                            
from apache_beam.io import WriteToText                                                                                                                                                                                                          
from apache_beam import window                                                                                                                                                                                                                  
                                                                                                                                                                                                                                                
default_topic = 'projects/<your-project>/topics/<your-topic>'                                                                                                                                                                            
default_bucket = 'gs://<gs-bucket-name>/data-team/sync/cs_bookings/'                                                                                                                                                                               
project = '<your-project-id>'                                                                                                                                                                                                               
                                                                                                                                                                                                                                                
WINDOW_LENGTH = 60*1 #60 secs *60 min                                                                                                                                                                                                           
                                                                                                                                                                                                                                                
                                                                                                                                                                                                                                                
def run(argv=None):                                                                                                                                                                                                                             
    parser = argparse.ArgumentParser()                                                                                                                                                                                                                                                                                                                                                                                                         
                                                                                                                                                                                                                                                
    known_args, pipeline_args = parser.parse_known_args(argv)                                                                                                                                                                                   
    pipeline_args.extend(['--project={}'.format(project)])                                                                                                                                                                                      
                                                                                                                                                                                                                                                
    pipeline_options = PipelineOptions(pipeline_args)                                                                                                                                                                                           
                                                                                                                                                                                                                                                
    def string_join(elements):                                                                                                                                                                                                                  
        return str('\n'.join(elements))                                                                                                                                                                                                         
                                                                                                                                                                                                                                                                                                                                                                                                                                                                               
                                                                                                                                                                                                                                                
    with beam.Pipeline(options=pipeline_options) as p:                                                                                                                                                                                          
        #current_topic = known_args.topic_prefix                                                                                                                                                                                                
        (p | "ReadTopic" >> beam.io.ReadFromPubSub(topic=default_topic)                                                                                                                                                                         
         | "Windowing" >> beam.WindowInto(window.FixedWindows(WINDOW_LENGTH))                                                                                                                                                                   
         | "Combine" >> beam.CombineGlobally(string_join).without_defaults()                                                                                                                                                                    
         | "WriteGCSCommon" >>                                                                                                                                                                                                                  
         WriteToText(file_path_prefix=default_bucket,shard_name_template='-SSSSS-of-NNNNN',file_name_suffix='YYYYMMDDHH'))                                                                                                                      
                                                                                                                                                                                                                                                
                                                                                                                                                                                                                                                
if __name__ == "__main__":                                                                                                                                                                                                                      
    logging.getLogger().setLevel(logging.INFO)                                                                                                                                                                                                  
    run()                                                                                                                                                                                                                                       
