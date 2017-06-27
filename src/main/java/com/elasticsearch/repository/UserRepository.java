package com.elasticsearch.repository;

import com.elasticsearch.entity.User;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.PagingAndSortingRepository;
import org.springframework.data.repository.query.Param;

import javax.transaction.Transactional;
import java.io.Serializable;

/**
 * 注：
 * 1、sql语句中不应该是表名，而是对应的实体类名称，如果想改动需要在实体类加上 @entity(name="user")
 * 2、写条件查询之时，都是 实体类名.实体类中的属性(不是数据库字段名)
 *  ?1代表第一个参数  ?2代表第二个参数
 * Created by Administrator on 2017/6/27.
 */
public interface UserRepository extends PagingAndSortingRepository<User,Serializable>{

    @Query("select u from user u where u.username = ?1")
//    @Query("select u from user u where u.username like %?1")
//    @Query(value = "select u from user u where u.username = ?1",
//            countQuery = "SELECT count(*)  from user u where u.username = ?1",
//            nativeQuery = true)
    Page<User> findUserByUsername(String username, Pageable pageable);

    @Query("select u from user u where u.message = :message")
//    @Query("select u from user u where u.message = ?")
//    @Query("select u from user u where u.message = :message")
//    @Query("select u from #{#entityName} u where u.message = ?1")
    User findUserByMessage(@Param("message") String message);

    @Transactional
    @Modifying
    @Query("update user u set u.message='1' where u.username=?1")
    int updateUserByUsername(String username);
}
