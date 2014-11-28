The Clerk database-project
---------------------------

Yet another database-engine with high hopes, maybe even potential and a founder with too much else to do. Should you read on? Even care?

Admitted this is still more dream than reality - but a beautiful dream none the less. Because:

- Document / Graph / Object style database (you know data-structures that can contain "direct" links to other data-structures)
- Lisp-inspired query interface brings even complex computations to the data-source.
- Event-driven architecture based on shared-nothing and no parallelism in user-code.
- MVCC lets you fork or branch the database trivially and instantly.
- Eventually consistent and leaning towards CRDT based coordination.
- Just as fast as anybody else (at least). Basic radix-tree-like data-structure means all operations are basically O(1) though the full story obviously also include IO-complexities etc. 

### OK, back to reality. Or: Were is the download-button then?

So yes the wonderful thing about open-source is that it's all out in the open. Just above this text if you are reading this on GitHub.

The project clearly mostly resembles a workshop were someones hobby-project lays scattered allover the floor. Because it is. So status on the above claims:

- Query-interface: In the source you currently find a compiler for a pascal-like input-language. Will soon die. So the new query-interface will just be data-structures describing computations (that could even create other computations) - yes, just like Lisp. But this still leaves us short of an interpreter. See next point.

- Event-driven. cle_stream.c implements a basic stateless gateway that accepts streams of hierarchal data-structures as input (like JSON or XML etc). It handles invocation-selection and role-based authentication. Kicks off the coming interpreter.

- MVCC. Lately all work has been on this feature. cle_task.c and cle_commit.c on top of a backend has the basic building-blocks for this to work. Still fighting the classic flood-of-history that arises from append-only persistent tree-structures. But the solution is in the workings. I promise. That solution will allow branches to co-exist and to be created on the fly.

- CRDT/Eventual consistent. In cle_task.c an implementation of a delta or change-set calculation exist. Based on the copy-on-write pages it deduces the changes (as insert and delete operations) implied. This change-set can trivially be sent to other nodes and applied there. Of cause at the risk of being out-of-sync, e.g. other changes have occurred at that node and these changes creates a conflict. So my plan is to try to do this as all CRDT's - so changes can always be applied (this is obviously a challenge and the solution *will* mean that this database might not work like others. For instance it could mean that all attributes might have potentially multiple versions and strange stuff like that)

- Speed. Its there. cle_struct.c implements the basic structure of the system. When all the pieces have been put together I will of cause publish rude and bragging micro-benchmarks that claim this to be the fastest in the world. Thats what all the others do, right? If you manage to build this you should run the test_main.c to get a feel for the level. Currently the code is portable C99 (or was intended to be). I will however do a version based on assembler instrinsics that will blow the rest of the fish out of the water :)

Thanks for taking the time to read this far! Ping me to encourage the work if you think any of this sounds (too) good (to be true).

 
With regards,

Lars Szuwalski  
2014

