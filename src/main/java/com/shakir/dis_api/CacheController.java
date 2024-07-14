package com.shakir.dis_api;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

@RestController
@RequestMapping("/cache")
public class CacheController {

    @Autowired
    private CacheService cacheService;

    @PutMapping("/put")
    public String put(@RequestParam String key, @RequestParam Integer value) {
        cacheService.put(key, value);
        return "Key-Value pair added.";
    }

    @GetMapping("/get")
    public Integer get(@RequestParam String key) {
        try {
            Future<Integer> result = cacheService.get(key);
            Integer value = result.get();
            if (value == null) {
                throw new KeyNotFoundException("Key not found: " + key);
            }
            return value;
        } catch (InterruptedException | ExecutionException e) {
            throw new KeyNotFoundException("Key not found: " + key);
        }
    }

    @DeleteMapping("/del")
    public String del(@RequestParam String key) {
        cacheService.del(key);
        return "Key-Value pair deleted.";
    }
}