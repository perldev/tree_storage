-module(bin_tree).
-export([create/2, open/2, get/2, put/3 ]).
-include_lib("kernel/include/file.hrl").

-record(handle,{
    index,
    source,
    file_info_index,
    file_info_storage,
    current_seek = 0,
    max_length_record = 1024
}).
-type handle() :: #handle{}.

%% @doc open existed  storage with binary

-spec open(list(), list()) -> handle().
open(Name, Options)->
    {ok,InfoStorage}  = file:read_file_info(Name),
    {ok, Device} = file:open(Name,[write, read, binary]),     
    {ok, Info}  = file:read_file_info("index_" ++Name),
    {ok, Device1} = file:open("index_" ++Name,[write, read, binary]), 
    MaxSize = case  proplists:get_value(max_length_record, Options)  of
                undefined ->
                        1024;
                Size when is_integer(Size)->
                        Size
            end,
    #handle{ index = Device1, max_length_record = MaxSize,
             source = Device, file_info_index = Info, file_info_storage = InfoStorage }
.
%% @doc create  storage with  binary tree

-spec create(list(), list()) -> handle().
create(Name, Options)->
    {ok, Device} = file:open(Name,[write, read, binary]),     
    {ok, Device1} = file:open("index_" ++Name,[write, read, binary]),     
    {ok, Info}  = file:read_file_info("index_" ++Name),
    {ok, InfoStorage}  = file:read_file_info(Name),
    MaxSize = case  proplists:get_value(max_length_record, Options)  of
                undefined ->
                        1024;
                Size when is_integer(Size)->
                        Size
            end,
    #handle{index = Device1,  max_length_record = MaxSize, 
            source = Device, file_info_index = Info, file_info_storage = InfoStorage}
.

%% @doc save key to the index return position to write in storage

-spec save_index(list(), binary()) -> handle().
save_index(Handle,  Key)->
    save_index(Handle, 0,   Key).
    
-spec save_index(handle(), integer(), binary() ) -> handle().
save_index(Handle, CurrentPos,  Key)->
        Device = Handle#handle.index,
        
        D = file:pread(Device, CurrentPos, 28 ),
%         io:format("read ~p  ~p ~n",[ {?MODULE,?LINE },{CurrentPos, Key, D}  ]),
        case D of
            eof -> 
               Info =  Handle#handle.file_info_index,
               InfoStorage =  Handle#handle.file_info_storage,
                file:pwrite(Device, CurrentPos,  <<Key/binary, 0:32, 0:32, 0:32>>  ),
                MaxSize = Handle#handle.max_length_record,
                { Handle#handle{file_info_index = Info#file_info{size = 28  },
                        file_info_storage = InfoStorage#file_info{size = MaxSize }
                },  0  }
               ;                  
            {ok, <<Key:16/binary, _LessPos:4/binary, _GreaterPos:4/binary, Value:4/binary>> } ->
                     <<Next:32>> = Value,
                     {Handle, Next };
            {ok, <<Key1:16/binary, LessPos:4/binary, GreaterPos:4/binary, Value:4/binary>> } ->
                
                case Key < Key1  of            
                    true -> 
                            <<Next:32>> = LessPos, 
                            case Next of
                                0 ->
                                    write_to_index(Handle, CurrentPos, Key1, LessPos, GreaterPos, Value, Key, less );
                                _ ->    
                                    save_index(Handle, Next,  Key)
                            end
                    ;
                    false ->
                            <<Next:32>> = GreaterPos,
                            case Next of
                                0 ->
                                    write_to_index(Handle, CurrentPos,  Key1, LessPos, GreaterPos, Value, Key, greater );
                                _ ->    
                                    save_index(Handle, Next,  Key)        
                            end                          
                end
        end
.

%% @doc write item to the index when find the index

write_to_index(Handle, CurrentPos, Key1, _LessPos, GreaterPos, Value, Key, less )->
        Info =  Handle#handle.file_info_index,
        Location  =  Info#file_info.size,
        InfoStorage =  Handle#handle.file_info_storage,
        NewValue  =  InfoStorage#file_info.size,
%         io:format("write ~p  ~p ~n",[ {?MODULE,?LINE },{CurrentPos, Key1, Location}  ]),
%         io:format("write new value of ~p  ~p ~n",[ {?MODULE,?LINE },{ Key, NewValue}  ]),
        MaxSize = Handle#handle.max_length_record,
        
        ok =   file:pwrite(Handle#handle.index, CurrentPos,  
                              <<Key1/binary, Location:32, GreaterPos/binary, Value/binary>>  ),
        ok =   file:pwrite(Handle#handle.index, Location,
                              <<Key/binary, 0:32, 0:32, NewValue:32 >> ), 
        { Handle#handle{file_info_index = Info#file_info{size = Location + 28  },
                        file_info_storage = InfoStorage#file_info{size = NewValue + MaxSize}
        },  NewValue  }
;
write_to_index(Handle, CurrentPos, Key1, LessPos, _GreaterPos, Value, Key, greater )->
        Info =  Handle#handle.file_info_index,
        Location  =  Info#file_info.size,
        InfoStorage =  Handle#handle.file_info_storage,
        NewValue  =  InfoStorage#file_info.size,
%         io:format("write ~p  ~p ~n",[ {?MODULE,?LINE },{CurrentPos, Key1, Location}  ]),
%         io:format("write new value of ~p  ~p ~n",[ {?MODULE,?LINE },{ Key, NewValue}  ]),
        MaxSize = Handle#handle.max_length_record,

        ok =   file:pwrite(Handle#handle.index, CurrentPos,  
                              <<Key1/binary, LessPos/binary, Location:32, Value/binary>> ),
        ok =   file:pwrite(Handle#handle.index, Location,
                              <<Key/binary, 0:32, 0:32, NewValue:32 >> ), 
        { Handle#handle{file_info_index = Info#file_info{size = Location + 28  },
                        file_info_storage = InfoStorage#file_info{size = NewValue + MaxSize}
                        }, NewValue }
.


%% @doc all record have constant length 
-spec make_record(list(), handle() ) -> binary().
make_record([Head|Value], Handle)->
        I = term_to_binary(Head),
        E = lists:foldl( fun(E, In)-> Val = term_to_binary(E) ,  <<In/binary,"_,_",Val/binary>>   end, I ,Value ),
        Size = erlang:byte_size(E),
        MaxSize = Handle#handle.max_length_record,
        Re = (MaxSize  - Size)*8,%%fill rest with nulls
        <<E/binary, 0:Re>>
.
%% @doc put new key to the storage
-spec put(handle(), list()|binary(), list()) -> handle().
put(Handle, PreKey, Value)->
          Key = crypto:hash(md5, PreKey),   
          BinValue = make_record(Value, Handle),
          { NewHandle, NewLocation } = save_index(Handle, Key),
          ok = file:pwrite(Handle#handle.source, NewLocation,  BinValue  ),
%           io:format("~p ~p ", [{?MODULE,?LINE}, Reason]),
          NewHandle
.

%%@doc get from position of ke from index

-spec get_from_index(handle(), list()|binary() ) -> integer().
get_from_index(Handle, Key)->
    get_from_index(Handle, 0, Key)
.

get_from_index(Handle, CurrentPos, Key ) ->
       Device = Handle#handle.index,
       D = file:pread(Device, CurrentPos, 28 ),
%        io:format("read ~p  ~p ~n",[ {?MODULE,?LINE },{CurrentPos, Key, D}  ]),

       case D of
            eof -> 
                     undefined;
            {ok, <<Key:16/binary, _LessPos:4/binary, _GreaterPos:4/binary, Value:4/binary>> } ->
                     <<Next:32>> = Value,
                     Next;
            {ok, <<Key1:16/binary, LessPos:4/binary, GreaterPos:4/binary, _Value:4/binary>> } ->
                case { Key < Key1, LessPos, GreaterPos  } of            
                     { true, <<0,0,0,0>>, _   }-> 
                         undefined;
                     { true, <<Next:32>> = LessPos, _   }-> 
                            get_from_index(Handle, Next, Key);
                     { false, _, <<0,0,0,0>> } ->
                            undefined;
                     { false, _ ,<<Next:32>> = GreaterPos  }->      
                            get_from_index(Handle, Next, Key)                 
                end
        end
.

%%@doc get key from the storage
-spec get(handle(), list()|binary()) ->   list() | undefined.
get(Handle, PreKey)->
        Key = crypto:hash(md5, PreKey),
        Location  = get_from_index(Handle, Key),
        read_from(Handle, Location)
.

read_from(_, undefined)->
        undefined;
read_from(Handle, Location)->       
        MaxSize = Handle#handle.max_length_record,
        {ok, Bin } = file:pread(Handle#handle.source, Location,  MaxSize ),
%         io:format("~p read ~p~n",[{?MODULE,?LINE}, Bin ]),
        List = binary:split(Bin, [ <<95,44,95>> ],[global]),
        read_items(List, []) 
       
.

read_items([], Acum)->
    Acum
;
read_items([Head|Tail], Acum)->
    Term = binary_to_term( Head ),
    read_items(Tail, Acum ++ [Term])
.


