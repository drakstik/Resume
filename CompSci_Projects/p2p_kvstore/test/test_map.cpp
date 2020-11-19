#include "../src/map.h"
#include <assert.h>

int main() {
    // A map with an initial capacity of one.
    Map* map = new Map(1);
    // String keys in our key-value pairs.
    String* k = new String("key");
    String* k2 = new String("key2");
    String* k3 = new String("key3");
    String* k4 = new String("key4");
    // String values in our key-value pairs.
    String* val1 = new String("value");
    String* val2 = new String("value");
    String* val3 = new String("value");
    String* val4 = new String("value");

    // Testing contains() and get() before puting any key-values in the map.
    assert(!map->contains(*k)); // returns false because there is no key k.
    assert(map->get(*k) == nullptr); // returns nullptr when map does not have the requested key k.

    // Putting k/v pairs into the map.
    map->put(*k, val1);
    map->put(*k2, val2); 
    map->put(*k3, val3);
    map->put(*k4, val4);

    // Testing contains() and get() after putting key value pairs
    assert(map->contains(*k)); // returns true because there is now a key k.
    assert(map->size() == 4);
    String* get_val4 = dynamic_cast<String*>(map->get(*k4));
    assert(get_val4 != nullptr);
    assert(get_val4->equals(val4));

    // Testing erase() after putting a key value pair (k, val1)
    map->erase(*k);

    assert(map->get(*k) == nullptr); // returns nullptr when map does not have the requested key s1.
    assert(map->size() == 3);

    delete map;
    delete k; delete k2; delete k3; delete k4;
    printf("Map tests passed.\n");
    return 0;
}