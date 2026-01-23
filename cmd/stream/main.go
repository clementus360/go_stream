package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/clementus360/go_stream/internal/api"
	streamHTTP "github.com/clementus360/go_stream/internal/handlers/http"
	pb_auth "github.com/clementus360/go_stream/internal/proto/auth"
	pb_db "github.com/clementus360/go_stream/internal/proto/stream"
	pb "github.com/clementus360/go_stream/internal/proto/stream_service"
	pb_user "github.com/clementus360/go_stream/internal/proto/user"
	"github.com/clementus360/go_stream/internal/service"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func main() {
	// 1. Redis Connection
	fmt.Printf("Redis Address: %s", os.Getenv("REDIS_ADDR"))
	rdb := redis.NewClient(&redis.Options{
		Addr:     os.Getenv("REDIS_ADDR"),
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("failed to connect to redis: %v", err)
	}

	// 2. Dial Clients (Auth, DB, User)
	// Using insecure for now; swap for mTLS later
	authConn, err := api.DialService(getEnv("AUTH_SERVICE_ADDR", "localhost:50052"), 5*time.Second)
	if err != nil {
		log.Fatalf("failed to dial auth service: %v", err)
	}
	dbConn, err := api.DialService(getEnv("DB_SERVICE_ADDR", "localhost:50053"), 5*time.Second)
	if err != nil {
		log.Fatalf("failed to dial db service: %v", err)
	}
	userConn, err := api.DialService(getEnv("USER_SERVICE_ADDR", "localhost:50054"), 5*time.Second)
	if err != nil {
		log.Fatalf("failed to dial user service: %v", err)
	}

	// 3. Initialize Service Manager
	mgr := service.NewStreamManager(
		pb_db.NewStreamServiceClient(dbConn),
		pb_auth.NewAuthServiceClient(authConn),
		pb_user.NewUserServiceClient(userConn),
		rdb,
	)

	// 4. Start gRPC Server (Port 50051)

	serverCreds, err := api.LoadServerTLSCredentials()
	if err != nil {
		log.Fatalf("CRITICAL: Failed to load server TLS credentials: %v", err)
	}

	go func() {
		lis, err := net.Listen("tcp", ":50051")
		if err != nil {
			log.Fatalf("gRPC listen failed: %v", err)
		}
		s := grpc.NewServer(grpc.Creds(serverCreds))
		pb.RegisterStreamManagerServer(s, mgr)
		reflection.Register(s)
		log.Println("🚀 gRPC Stream Server running on :50051")
		if err := s.Serve(lis); err != nil {
			log.Fatalf("gRPC serve failed: %v", err)
		}
	}()

	// 5. Start HTTP Server (Port 8080)
	httpHandler := streamHTTP.NewStreamHTTPHandler(mgr)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/streams/live", httpHandler.GetLiveStreams)
	mux.HandleFunc("POST /v1/streams/metadata", httpHandler.UpdateMetadata)

	log.Println("🌐 HTTP Discovery API running on :8080")
	if err := http.ListenAndServe(":8080", streamHTTP.CORSMiddleware(mux)); err != nil {
		log.Fatalf("HTTP serve failed: %v", err)
	}
}

// getEnv is a helper to allow default values if environment variables are missing
func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}
