package com.codejiwei.gmall.controller;

import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @ClassName FirstController
 * @Description TODO
 * @Author codejiwei
 * @Date 2021/1/29 14:20
 * @Version 1.0
 **/
@RestController
public class FirstController {
    @RequestMapping("/first")
    public String test(){
        return "This is first";
    }
}
