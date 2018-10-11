# test-task

On Task#1:
Session boundaries defined as either previous event is null, meaning session just started, or difference between events
is greater than some defined value (5 minutes in task#1).

On Task#2.1:
Median session duration were easy to express using just SQL query.

On Task#2.2:
I assumed that I should use sessions defined in Task#2 to count time, effectively meaning that users cannot have sessions
across these defined Task#1 sessions. Time spent counts as sum of differences between events for simplification. I think
that there can be smarter session boundaries that depend on type of action and/or next user action. 

On user sessions for Task#2.3:
I assumed session start will be the first event on product and session end will be the last event. At first I did that 
the next product event will be the end of session, but looking at data it seems unreasonable. Probably it is better to
set the session end by the type of last action on the product and within external sessions (say, we assuming that end
of session is either start of next product action or last event time if 5 minutes passed since last event on it).

