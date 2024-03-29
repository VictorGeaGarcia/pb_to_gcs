import argparse                                                                                                                                                                                                                                 
import logging                                                                                                                                                                                                                                  
                                                                                                                                                                                                                                                
import apache_beam as beam                                                                                                                                                                                                                      
from apache_beam import pvalue                                                                                                                                                                                                                  
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

from apache_beam.io.gcp.pubsub import ReadFromPubSub                                                                                                                                                                                            
from apache_beam.io import WriteToText                                                                                                                                                                                                          
from apache_beam import window                                                                                                                                                                                                                  
                                                                                                                                                                                                                                                
default_topic = 'projects/<your-project>/topics/<your-topic>'                                                                                                                                                                           
default_bucket = 'gs://<gs-bucket-name>/data-team/sync/'                                                                                                                                                                               
project = 'main-training-project'                                                                                                                                                                                                               
                                                                                                                                                                                                                                                
WINDOW_LENGTH = 60*1 #60 secs *60 min                                                                                                                                                                                                           
                                                                                                                                                                                                                                          
def run(argv=None):                                                                                                                                                                                                                             
    parser = argparse.ArgumentParser()                                                                                                                                                                                                                                                                                                                                                                                              
                                                                                                                                                                                                                                                
    known_args, pipeline_args = parser.parse_known_args(argv)                                                                                                                                                                                   
    pipeline_args.extend(['--project={}'.format(project)])                                                                                                                                                                                      
                                                                                                                                                                                                                                                
    pipeline_options = PipelineOptions(pipeline_args)                                                                                                                                                                                           
    pipeline_options.view_as(SetupOptions).save_main_session = True
    
    class DiffOutputsFn(beam.DoFn):                                                                                                                                                                                                             
        # These tags will be used to tag the outputs of this DoFn.                                                                                                                                                                              
        OUTPUT_TAG_CS_BOOKINGS = 'tag_cs_bookings'                                                                                                                                                                                              
        OUTPUT_TAG_USERS = 'tag_users'                                                                                                                                                                                                          
        from apache_beam import pvalue                                                                                                                                                                                                          
        def process(self, element):                                                                                                                                                                                                             
            #Receives a single element (a line) and produces words and character                                                                                                                                                                
            #counts.                                                                                                                                                                                                                            
            if 'vehicle_id' in element:                                                                                                                                                                                                         
                yield pvalue.TaggedOutput(self.OUTPUT_TAG_CS_BOOKINGS,element)
            else:                                                                                                                                                                                                                               
                yield pvalue.TaggedOutput(self.OUTPUT_TAG_USERS,element)                                                                                                                                                                        
    
    
    def string_join(elements):                                                                                                                                                                                                                  
        return str('\n'.join(elements))                                                                                                                                                                                                         
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                
                                                                                                                                                                                                                                                
    with beam.Pipeline(options=pipeline_options) as p:                                                                                                                                                                                          

        diff_outputs = (p | "ReadTopic" >>                                                                                                                                                                                                      
                        beam.io.ReadFromPubSub(topic=default_topic) |                                                                                                                                                                           
                        "SplitOutputs" >> beam.ParDo(DiffOutputsFn()).with_outputs(DiffOutputsFn.OUTPUT_TAG_CS_BOOKINGS,DiffOutputsFn.OUTPUT_TAG_USERS))                                                                                        
        cs_bookings_p = (diff_outputs.tag_cs_bookings                                                                                                                                                                                           
         | "Windowing_cs_bookings" >> beam.WindowInto(window.FixedWindows(WINDOW_LENGTH))                                                                                                                                                       
         | "Combine_cs_bookings" >> beam.CombineGlobally(string_join).without_defaults()                                                                                                                                                        
         | "WriteGCSCommon_cs_bookings" >> WriteToText(file_path_prefix=default_bucket+'cs_bookings/',file_name_suffix='cs_booking_YYYYMMDDHH'))                                                                                                               
        
        users_p = (diff_outputs.tag_users                                                                                                                                                                                                 
         | "Windowing_users" >> beam.WindowInto(window.FixedWindows(WINDOW_LENGTH))                                                                                                                                                             
         | "Combine_users" >> beam.CombineGlobally(string_join).without_defaults()                                                                                                                                                              
         | "WriteGCSCommon_users" >> WriteToText(file_path_prefix=default_bucket+'users/',file_name_suffix='users_YYYYMMDDHH'))                                                                                                                       
                                                                                                                                                                                                                                                
                                                                                                                                                                                                                                                
if __name__ == "__main__":                                                                                                                                                                                                                      
    logging.getLogger().setLevel(logging.INFO)                                                                                                                                                                                                  
    run() 
