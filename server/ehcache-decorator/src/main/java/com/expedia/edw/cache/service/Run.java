package com.expedia.edw.cache.service;

import com.expedia.edw.cache.dao.GrabberDao;
import com.expedia.edw.cache.dao.GrabberDaoJDBC;

/**
 *
 * @author Yaroslav_Mykhaylov
 */
public class Run {
    public static void main(String[] args){
        GrabberDao grabberDao = new GrabberDaoJDBC();
        System.out.println(grabberDao.getData("edw_cache.users_0", "id_user", "name"));
    }
}
