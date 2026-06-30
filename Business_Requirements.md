Thanks for taking the time to talk with us. As a next step we'd like you to work through a short, real-world exercise.

What you're getting
Two Python scripts that power a "provider network comparison" feature: one produces provider counts (provider_count_assignment.py), the other generates downloadable detail files (provider_download_assignment.py).
A data generator (generate_dummy_data.py) and requirements.txt. Running the generator creates a local sample_data/ tree so you can run the scripts end-to-end. 

How the flow works today
Each user request spawns these scripts as subprocesses. They read provider files (parquet + CSV) from disk at request time, do all the computation in-process, and write results to SQL Server. This works, but it's slow and doesn't scale well as concurrent usage grows.

The exercise 
Your call on the approach We're deliberately not prescribing a solution. We'd like you to:
Understand the current flow and call out its main problems (correctness, performance, scalability, maintainability, security  whatever you find).
Decide how you'd improve it. Optimising the Python is one option; re-architecting the flow in a different way is equally valid. We're most interested in how you reason about the trade-offs and justify your choice.
Demonstrate your approach with whatever depth time allows -  a working prototype, a partial implementation, pseudocode, and/or a written design. A clear design with sound reasoning is more valuable than a rushed full rewrite.
What to submit
 Your code/changes (if any) plus a short write-up covering: what you found, the approach you chose and why, what you'd do with more time, and any assumptions. A Git repo link is fine.

If anything's unclear, just ask. Looking forward to seeing how you think about this.