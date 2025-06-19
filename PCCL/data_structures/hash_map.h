#ifndef _HASH_MAP_H_
#define _HASH_MAPS_H_

#include <iostream>
#include <unordered_map>
#include <mutex>

template <class _key, class _val>
class SpinLockMap
{
public:
    SpinLockMap(){};
    ~SpinLockMap() { this->map.clear(); }

    bool exists(_key key)
    {
        bool result = false;
        this->lock.lock();
        result = this->map.count(key);
        this->lock.unlock();
        return result;
    }
    bool check_and_get(_key key, _val &val)
    {
        bool result = false;
        this->lock.lock();
        result = this->map.count(key);
        if (result)
            val = this->map[key];
        this->lock.unlock();
        return result;
    }
    _val get(_key key)
    {
        this->lock.lock();

        assert(this->map.count(key));
        _val temp = this->map[key];

        this->lock.unlock();
        return temp;
    };

    void add(_key key, _val value)
    {
        this->lock.lock();
        this->map[key] = value;
        this->lock.unlock();
    };
    int remove(_key key)
    {
        if (this->mutex_map.count(key))
        {
            this->mutex_lock.lock();
            std::mutex *element_mutex = this->mutex_map[key];
            element_mutex->lock();
            delete element_mutex;
            this->mutex_map.erase(key);
        }
        this->lock.lock();

        int result = this->map.erase(key);

        this->lock.unlock();
        return result;
    }
    _val pop(_key key)
    {
        _val temp;
        if (this->mutex_map.count(key))
        {
            assert(0);
            this->mutex_lock.lock();
            std::mutex *element_mutex = this->mutex_map[key];
            element_mutex->lock();
            delete element_mutex;
            this->mutex_map.erase(key);
        }
        this->lock.lock();
        if (this->map.count(key))
        {
            temp = this->map[key];
            this->map.erase(key);
        }
        else
        {
            temp = 0;
        }
        this->lock.unlock();
        return temp;
    }
    _val get_and_lock(_key key)
    {
        this->lock.lock();
        this->mutex_lock.lock();

        assert(this->map.count(key));
        _val temp = this->map[key];
        std::mutex *element_mutex = new std::mutex();
        element_mutex->lock();
        mutex_map[key] = element_mutex;

        this->lock.unlock();
        this->mutex_lock.unlock();
        return temp;
    }
    _val unlock(_key key)
    {
        this->mutex_lock.lock();

        assert(this->mutex_map.count(key));
        std::mutex *element_mutex = this->mutex_map[key];
        element_mutex->unlock();

        this->mutex_lock.unlock();
    }

    int size()
    {
        // assert(map.size() >= mutex_map.size());
        return map.size();
    }

private:
    std::unordered_map<_key, _val> map;
    std::unordered_map<_key, std::mutex *> mutex_map;
    std::mutex lock;
    std::mutex mutex_lock;
};

#endif