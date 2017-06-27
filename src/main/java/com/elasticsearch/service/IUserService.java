package com.elasticsearch.service;

import com.elasticsearch.entity.User;

import java.io.Serializable;

/**
 * JpaSpecificationExecutor<User>
 * spring data jpa为我们提供了JpaSpecificationExecutor接口，只要简单实现toPredicate方法就可以实现复杂的查询
 * Created by Administrator on 2017/6/27.
 */
public interface IUserService{
    public User getUserById(Serializable id);
    public int updateUserByUsername(String username);

    public User findUserByMessage(String message);
}
