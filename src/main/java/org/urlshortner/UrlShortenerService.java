package org.urlshortner;

public class UrlShortenerService {
    private static final String BASE62 = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    private static final String BASE_URL = "https://short.ly/";
    private static final int MAX_RETRY_ATTEMPTS = 3;
    
    private final DistributedIDGenerator idGenerator;
    private final DatabaseService database;
    
    public UrlShortenerService(long serverId, long rangeSize) {
        // Use SimpleRedisClient for in-memory distributed ID generation
        SimpleRedisClient redisClient = new SimpleRedisClient();
        
        // Initialize distributed ID generator and database
        this.idGenerator = new DistributedIDGenerator(serverId, rangeSize, redisClient);
        this.database = new DatabaseService();
    }
    
    public String shortenUrl(String originalUrl) {
        for (int attempt = 0; attempt < MAX_RETRY_ATTEMPTS; attempt++) {
            // Get unique ID from distributed generator
            long id = idGenerator.getNextId();
            
            // Convert to Base62
            String shortCode = encodeBase62(id);
            
            // Try to store mapping with collision detection
            if (storeMapping(shortCode, originalUrl)) {
                return BASE_URL + shortCode;
            }
            
            System.out.println("Collision detected for: " + shortCode + ", retrying...");
        }
        
        throw new RuntimeException("Failed to generate unique short code after " + MAX_RETRY_ATTEMPTS + " attempts");
    }
    
    private String encodeBase62(long num) {
        if (num == 0) return String.valueOf(BASE62.charAt(0));
        
        StringBuilder result = new StringBuilder();
        while (num > 0) {
            result.append(BASE62.charAt((int)(num % 62)));
            num /= 62;
        }
        return result.reverse().toString();
    }
    
    private boolean storeMapping(String shortCode, String originalUrl) {
        boolean success = database.storeMapping(shortCode, originalUrl);
        if (success) {
            System.out.println("Stored: " + shortCode + " -> " + originalUrl);
        }
        return success;
    }
    
    public String expandUrl(String shortCode) {
        return database.getOriginalUrl(shortCode);
    }
    
    public static void main(String[] args) {
        // Create URL shortener service for server 1
        UrlShortenerService service = new UrlShortenerService(1, 1000);
        
        // Shorten some URLs
        String shortUrl1 = service.shortenUrl("https://www.google.com");
        String shortUrl2 = service.shortenUrl("https://www.amazon.com");
        
        System.out.println("Short URL 1: " + shortUrl1);
        System.out.println("Short URL 2: " + shortUrl2);
    }
}