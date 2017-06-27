package com.elasticsearch.service.impl;

import com.elasticsearch.entity.User;
import com.elasticsearch.repository.UserRepository;
import com.elasticsearch.service.IUserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.Serializable;
import java.lang.invoke.SerializedLambda;

/**
 * Created by Administrator on 2017/6/27.
 */
@Service
public class UserServiceImpl implements IUserService{

    @Autowired
    private UserRepository userRepository;

    @Override
    public User getUserById(Serializable id) {
        return userRepository.findOne(id);
    }

    @Override
    public int updateUserByUsername(String username) {
        return userRepository.updateUserByUsername(username);
    }

    @Override
    public User findUserByMessage(String message) {
        return userRepository.findUserByMessage(message);
    }
}
