Authalog is a datalog engine designed to support flexible, debuggable and performant authorization systems.
Authalog supports datalog with limited negation, and uses top-down, tabling searh strategy.

Things that worked:
 * Data provenance tracking
 * Caching and invalidation of derived facts
 * Use of a backing relational store
 * Datalog supports a wide range of authorization models -- see the examples (there were others that lived outside this repository)
 * Use datalog through a library interface embedded in another language, with full support for a text representation of rules and queries. 
 * Lifting authorization into a relational domain does allow us to enumerate and query much more information about who has access to what datums.

Things that didn't:
 * Debugging was still a pain -- provenance only has the ability to explain positive facts, not explain failures.
 * Many of the datatypes that need to be manipulated in authorization systems are enums that then end up beign switched over. Unlike in other programming languages, expressing this in datalog is clumsy (I introduced the inset/2 built-in predicate, and set datatype to make this switching somewhat easier). However, even with this, it was difficult to be confident that all branches of a given switch had been covered, as they tended to be split across several predicate definitions.
 * A lot of rules are best expressed as simple tables -- this remained clumsy, although probably could have been addressed with a simple csv/xlsx -> datalog translator.
 * It's not clear that full recursion is necessary.

Next steps:
 * I have no plans to maintain authalog -- it was an interesting experiment, but I believe ultimately flawed.

Installation:
    The examples use sqllite3, which depends on cgo. This means you need to have gcc installed -- consider,
    for windows dev machines, installing it from here: http://tdm-gcc.tdragon.net/download
    for linux: sudo apt-get install gcc

Bugs:
 * There are probably many.