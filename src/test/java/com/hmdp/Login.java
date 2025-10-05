package com.hmdp;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import cn.hutool.core.lang.UUID;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;
import com.hmdp.service.impl.UserServiceImpl;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;

import javax.annotation.Resource;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.LOGIN_USER_KEY;
import static com.hmdp.utils.RedisConstants.LOGIN_USER_TTL;
@SpringBootTest
public class Login {
    @Resource
    private UserServiceImpl userService;
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Test
    void login() {
        for(int i=1;i<=1010;i++){
            User user = userService.getById(i);
            if(user==null){
                continue;
            }
            //7.保存用户信息到Redis
            //7.1.随机生成token作为登录令牌
            String token = UUID.randomUUID().toString(true);
            //7.2.将User对象转为hashmap存储
            UserDTO userDTO= BeanUtil.copyProperties(user, UserDTO.class);
            Map<String, Object> userMap = BeanUtil.beanToMap(userDTO,new HashMap<>(),
                    CopyOptions.create().setIgnoreNullValue(true).setFieldValueEditor((fieldName, fieldValue)->fieldValue.toString()));
            //7.3.存储
            String tokenKey=LOGIN_USER_KEY+token;
            stringRedisTemplate.opsForHash().putAll(tokenKey,userMap);
            //7.4.设置有效期
            stringRedisTemplate.expire(tokenKey,LOGIN_USER_TTL, TimeUnit.MINUTES);
        }

    }

    @Test
    void readAllTokens(){
        String keyPattern = "login:token:*";
        String outputFilePath = "src/main/resources/tokens.txt"; // 输出文件路径

        // 获取所有匹配的 key
        Set<String> keys = stringRedisTemplate.keys(keyPattern);
        if (keys == null || keys.isEmpty()) {
            System.out.println("没有找到匹配的 key。");
            return;
        }

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFilePath))) {
            for (String key : keys) {
                // 提取 login:token: 后的部分
                if (key.startsWith("login:token:")) {
                    String token = key.substring("login:token:".length());
                    writer.write(token);
                    writer.newLine();
                }
            }
            System.out.println("已成功将 token 导出到文件：" + outputFilePath);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
