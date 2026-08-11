package post;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication(scanBasePackages = {"post", "sku"})
@MapperScan("post.mapper")
@EnableScheduling
public class ShareAgentApplication {
    public static void main(String[] args) {
        SpringApplication.run(ShareAgentApplication.class, args);
    }
}