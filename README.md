# test-task

On user sessions:
I assumed session start will be the first event on product 
and session end will be the last event. At first I did that 
the next product event will be the end of session, but looking
at data it seems unreasonable. Probably it is better to set the
session end by the type of last action on the product and within 
external sessions (say, we assuming that end of session is either
start of next product action or last event time if 5 minutes
passed since last event on it) but I hadn't time for it.
