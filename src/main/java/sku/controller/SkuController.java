package sku.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import sku.service.SkuService;

@RestController
@RequestMapping("/sku")
public class SkuController {
    @Autowired
    SkuService skuService;

    @PostMapping("/fav")
    public boolean fav(
            @RequestParam Long skuId,
            @RequestParam Long userId,
            @RequestParam Boolean isFav
    ) {
        return skuService.fav(skuId,userId,isFav);
    }

    @PostMapping("/buy")
    public Long buy(
            @RequestParam Long skuId,
            @RequestParam Long userId
    ) {
        return skuService.buy(skuId, userId);
    }
}
