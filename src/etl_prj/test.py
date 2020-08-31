log_insert="""
insert into log (
 song_title ,
 session_id, 
 location, 
 user_agent) values (%s,%s, %s, %s) 
"""