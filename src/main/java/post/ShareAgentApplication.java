package post;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = {"post", "sku"})
@MapperScan("post.mapper")
public class ShareAgentApplication {
    public static void main(String[] args) {
        SpringApplication.run(ShareAgentApplication.class, args);
    }
}