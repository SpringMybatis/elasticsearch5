package com.elasticsearch.controller;

import com.elasticsearch.entity.User;
import com.elasticsearch.service.IUserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Created by Administrator on 2017/6/27.
 */
@RestController
@RequestMapping("/user")
public class UserController {

    @Autowired
    private IUserService userService;

    @GetMapping("/{id}")
    public User getUserById(@PathVariable Integer id){
        return userService.getUserById(id);
    }

    @GetMapping("/update")
    public int updateUserByUsername(){
        return userService.updateUserByUsername("miao");
    }

    @GetMapping("/find")
    public User findUserByMessage(){
//        Sort sort = new Sort(Direction.DESC,"username");
//        PageRequest request = new PageRequest(1,10);
        return userService.findUserByMessage("1");
    }
}
