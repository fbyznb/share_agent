package sku.service;

public interface SkuService {
    boolean fav(Long skuId,Long userId,Boolean isFav);
    Long buy(Long skuId,Long userId);
}
