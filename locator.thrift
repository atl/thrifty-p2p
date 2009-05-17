struct Location {
 1: string address,
 2: i16 port,
}

service Locator {
 list<Location> join    (1:Location location)
 void           remove  (1:Location location, 2:list<Location> authorities)
 void           add     (1:Location location, 2:list<Location> authorities)
 list<Location> get_all ()
 Location       get_node(1:string key)
 void           ping    ()
 void           debug   ()
}