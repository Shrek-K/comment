package com.hmdp.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.Collections;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
@Slf4j
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Autowired
    //@Resource
    private ISeckillVoucherService seckillVoucherService;
    @Autowired
    private RedisIdWorker redisIdWorker;
    @Autowired
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private RedissonClient redissonClient;
    public static final DefaultRedisScript<Long> SECKILL_SCRIPT;
    static {
        SECKILL_SCRIPT=new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    private BlockingQueue<VoucherOrder> orderTasks=new ArrayBlockingQueue<>(1024 * 1024);
    //线程池
    private static final ExecutorService SECKILL_ORDER_EXECUTOR= Executors.newSingleThreadExecutor();
    //确定什么时候执行
    @PostConstruct//在当前类初始化完毕后执行
    private void init(){
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }

    //线程任务
    private class VoucherOrderHandler implements Runnable{
        @Override
        public void run() {
            //循环取出任务
            while (true){
                try {
                    //1.获取队列中的订单信息
                    VoucherOrder voucherOrder = orderTasks.take();//获取队列中的头部，如果有需要则等待直到有元素
                    //2.创建订单
                    handleVoucherOrder(voucherOrder);
                } catch (InterruptedException e) {
                    log.error("处理订单异常",e);
                }
            }
        }
    }

    private void handleVoucherOrder(VoucherOrder voucherOrder) {
        //1.获取用户
        Long userId = voucherOrder.getUserId();
        //可以不需要锁，因为Lua脚本已经确保不会出现重复的了，这里做是兜底
        //2.创建锁对象
        RLock lock = redissonClient.getLock("lock:order:" + userId);
        //3.获取锁
        boolean isLock = lock.tryLock();
        //4.判断是否获取锁成功
        if(!isLock){
            //获取锁失败，返回错误
            log.error("不允许重复下单");
            return ;
        }
        try {
            //线程的代理对象
            proxy.createVoucherOrder(voucherOrder);
        } finally {
            //释放锁
            lock.unlock();
        }
    }
    private IVoucherOrderService proxy;
    @Override
    public Result seckillVouvher(Long voucherId) {
        //获取用户
        Long userId=UserHolder.getUser().getId();
        //1.执行lua脚本
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(),
                userId.toString()
        );
        int r = result.intValue();
        //2.判断结果是否为0
        if(r!=0){
            //2.1.不为0没有购买资格
            return Result.fail(r==1?"库存不足":"不能重复下单");
        }

        //2.2.为0有购买资格，下单信息保存阻塞队列
        VoucherOrder voucherOrder = new VoucherOrder();
        //2.3.订单id
        long orderId = redisIdWorker.nextId("order");
        voucherOrder.setId(orderId);
        //2.4.用户id
        userId = UserHolder.getUser().getId();
        voucherOrder.setUserId(userId);
        //2.5.代金券id
        voucherOrder.setVoucherId(voucherId);
        //2.6创建阻塞队列
        orderTasks.add(voucherOrder);
        //3.获取代理对象（事务）
        proxy = (IVoucherOrderService) AopContext.currentProxy();
        //4.返回订单id
        return Result.ok(orderId);
    }
    @Transactional
    public void createVoucherOrder(VoucherOrder voucherOrder) {
        //5.一人一单
        Long userId =voucherOrder.getUserId();
        //5.1.查询订单
        Long count = query().eq("user_id", userId).eq("voucher_id", voucherOrder.getVoucherId()).count();
        //5.2.判断是否存在
        if (count > 0) {
            //用户已经购买
            log.info("该用户已经购买过一次");
            return;
        }
        //6.扣减库存
        boolean succcess = seckillVoucherService.update()
                .setSql("stock=stock-1")//set stock =stock-1
                .eq("voucher_id", voucherOrder.getVoucherId()).gt("stock", 0)//where id = ? and stock > 0
                .update();
        if (!succcess) {
            //扣减失败
            log.info("库存不足！");
            return ;
        }
        //7.创建订单
        save(voucherOrder);
    }
    /**
     * 秒杀业务
     */
    /*@Override
    public Result seckillVouvher(Long voucherId) {
        //1.查询优惠券信息
        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
        //2.判断秒杀是否开始
        if (voucher.getBeginTime().isAfter(LocalDateTime.now())) {
            //秒杀未开始
            return Result.fail("秒杀尚未开始！");
        }
        //3.判断秒杀是否结束
        if (voucher.getEndTime().isBefore(LocalDateTime.now())) {
            //秒杀已结束
            return Result.fail("秒杀已经结束！");
        }
        //4.判断库存是否充足
        if (voucher.getStock() < 1) {
            //库存不足
            return Result.fail("库存不足！");
        }
        Long userId = UserHolder.getUser().getId();
        //synchronized (userId.toString().intern()) {
        //创建锁对象
        //SimpleRedisLock lock = new SimpleRedisLock("order:" + userId, stringRedisTemplate);
        RLock lock = redissonClient.getLock("lock:order:" + userId);
        //获取锁
        boolean isLock = lock.tryLock();
        //判断是否获取锁成功
        if(!isLock){
            //获取锁失败，返回错误
            return Result.fail("不允许重复下单！");
        }
        try {
            //获取代理对象（事务）
            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
            return proxy.createVoucherOrder(voucherId);
        } finally {
            //释放锁
            lock.unlock();
        }
        //}
    }
    @Transactional
    public Result createVoucherOrder(Long voucherId) {
        //5.一人一单
        Long userId = UserHolder.getUser().getId();
        //5.1.查询订单
        Long count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
        //5.2.判断是否存在
        if (count > 0) {
            //用户已经购买
            return Result.fail("该用户已经购买过一次！");
        }
        //6.扣减库存
        boolean succcess = seckillVoucherService.update()
                .setSql("stock=stock-1")//set stock =stock-1
                .eq("voucher_id", voucherId).gt("stock", 0)//where id = ? and stock > 0
                .update();
        if (!succcess) {
            //扣减失败
            return Result.fail("库存不足！");
        }
        //7.创建订单
        VoucherOrder voucherOrder = new VoucherOrder();
        //7.1.订单id
        long orderId = redisIdWorker.nextId("order");
        voucherOrder.setId(orderId);
        //7.2.用户id
        userId = UserHolder.getUser().getId();
        voucherOrder.setUserId(userId);
        //7.3.代金券id
        voucherOrder.setVoucherId(voucherId);
        save(voucherOrder);
        //8.返回订单id
        return Result.ok(orderId);
    }
*/
}
