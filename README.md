tree_storage
============

Simple file storage based on binary  tree index

to create storage


> Handle = bin_tree:create("my_first_tree_storage", [ {max_length_record, 300} ]).


This call will return you a handle of storage, the storage you have created by this
call is able to handle list with max length 300 bytes


> Handle2 =  bin_tree:put(Handle, "key", [ first, second, next ] ).

> Handle3 =  bin_tree:put(Handle2, "key1", [ first1, second2, next ] ).

> Handle4 =  bin_tree:put(Handle3, "key2", [ first1, second3, next ] ).

> Handle5 =  bin_tree:put(Handle4, "key3", [ first1, second4, next ] ).


> Value =  bin_tree:get(Handle5, "key3").


>[ first1, second4, next ]


This time we are able store only lists.













