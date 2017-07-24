Regen
=====

In this section, we present Flamy's most powerful feature: the ``regen``
This feature is not part of the open-source edition of Flamy, so please 
`contact us <http://www.flaminem.com/en/contact-us>`_ if you would like to try it out.

After two years developing and using flamy, we obtained 
very high increase in productivity when using Hive, but one of the 
most time-consuming issue we still had to deal with was 
making sure that our constantly-evolving data pipelines 
were always correct and up to date.

Imagine that you start with a simple data pipeline
such as the one we used in the :doc:`tutorial <Demo>`:

.. image:: https://github.com/flaminem/flamy-demo/raw/master/images/graph%20nasa_access.png
   :alt: nasa_access graph

Motivation
""""""""""

Let us take as an example the simple data pipeline used in [flamy's tutorial](Demo).
The input table of your workflow (here nasa_access.daily_logs) receives data from an external source and you execute various
transformations to obtain other tables. Your tables are partitioned by day, and every day, you execute 
the workflow with the ``flamy run`` command for the new batch of data that arrived.

But at some point, you realize that because of some error somewhere,
the last two months of data that arrived in the table nasa_access.daily_logs
contains some error that should be fixed (for instance, a field has a wrong format, and this 
generates null values down your pipeline). 
What should you do?

Generally, the solution consists in writing a custom query to fix the issue over the last
two months, and of course update the table's Populate to prevent this problem from occurring again.

But once you have applied your fix and regenerated two months of data in the table nasa_access.daily_logs,
you should perhaps propagate the changes downstream and recompute two months of data for the other tables
below.  
And this is where we hit a complex issue: how to check which partitions are up-to-date with their upstream,
and which one should be regenerated?
In our simple example we have only 4 tables, but quite frequently companies can build very complex pipelines 
that can have tens of intermediary steps, branches, and so on. Imagine the kind of conundrums it can create!

This is why we added to flamy the ``regen`` feature:

regen
"""""

By analyzing the Hive queries, flamy is actually able to infer which partitions are outdated or missing 
regarding their upstream data. 
Flamy is then capable of running the right POPULATE queries, with the correct variable replacements,
to (re-)generated the outdated or missing partitions in your workflow.

Here is an example of the regen running on the tutorial's example:

.. raw:: html

  <script type="text/javascript" src="https://asciinema.org/a/tB7LPJFcxCFZZP3ca6ARWdipd.js" id="asciicast-tB7LPJFcxCFZZP3ca6ARWdipd" async></script>

With flamy's regen, you can now rest assured that your data pipeline is entirely coherent.

This is also extremely useful for failure recovery: if you run a workflow with flamy's regen, 
and one of the query fails for some reason, then by relaunching the command, 
flamy will automatically skip the queries that were successfully completed the first time.

**All this, without having to write any workflow description: just your SQL queries and one flamy command.**

This feature is not part of the open-source edition of Flamy, please
`contact us <http://www.flaminem.com/en/contact-us>`_ if you are interested and want to know more.
