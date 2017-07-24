Flamy Overview
==============

What Flamy is
-------------

Flamy is a command line tool designed to make all people using SQL on Hadoop being more productive.

It does so by bringing multiple functionalities, that allows SQL developers to:
- easily and rapidly check the integrity of their queries, even against an evolving database
- better visualize and understand the workflows they created
- easily deploy and execute them on multiple environments
- efficiently gather metadata from the Metastore


SQL is often recognized to be a powerful language to script and automate queries, 
while at the same time maintaining and improving running workflow can often become quite frustrating. 
The fact that it is not a compiled language and cannot be easily unit tested is often cited
as a main downside when compared to other approaches such as using plain Spark in java or scala.

Flamy's philosophy is to remove such downsides, and allow users to make the most out of SQL on Hadoop,
without forcing them to use SQL for tasks it is not great at.



What Flamy is not
-----------------

Flamy is not a GUI
""""""""""""""""""
Flamy doesn't come with fancy graphical sugar, yet. Sorry.


Flamy is not a scheduler
""""""""""""""""""""""""
It can execute workflows of consecutive Hive-SQL queries,
and we plan to add more capabilities such as running Presto queries or Spark jobs,
but it has no cron-like capabilities and is not intended to have someday.

Flamy is best used in conjunction with your favorite scheduler, 
either by using flamy to generate a workflow and export it into the scheduler's format, 
or by having the scheduler directly calling flamy commands.
We encourage the community to contribute by building such bridges 
between flamy and other great open source schedulers.


  

