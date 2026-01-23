package service

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	pb_auth "github.com/clementus360/go_stream/internal/proto/auth"
	pb_db "github.com/clementus360/go_stream/internal/proto/stream"
	pb "github.com/clementus360/go_stream/internal/proto/stream_service"
	pb_user "github.com/clementus360/go_stream/internal/proto/user"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

type StreamManager struct {
	pb.UnimplementedStreamManagerServer
	DBClient   pb_db.StreamServiceClient
	AuthClient pb_auth.AuthServiceClient
	UserClient pb_user.UserServiceClient
	Redis      *redis.Client
}

// LiveCache includes all fields supported by our DB schema and Proto
type LiveCache struct {
	ChannelID   int32  `json:"channel_id"`
	SessionID   int64  `json:"session_id"`
	Username    string `json:"username"`
	Title       string `json:"title"`
	Description string `json:"description"`
	Resolution  string `json:"resolution"`
}

func NewStreamManager(db pb_db.StreamServiceClient, auth pb_auth.AuthServiceClient, user pb_user.UserServiceClient, rdb *redis.Client) *StreamManager {
	return &StreamManager{
		DBClient:   db,
		AuthClient: auth,
		UserClient: user,
		Redis:      rdb,
	}
}

func (s *StreamManager) StartStream(ctx context.Context, req *pb.StartStreamRequest) (*pb.StartStreamResponse, error) {

	// 1. Authorization
	authResp, err := s.AuthClient.AuthorizeStream(ctx, &pb_auth.AuthorizeStreamRequest{
		StreamKey: req.StreamKey,
	})
	if err != nil {
		return nil, status.Errorf(codes.Unauthenticated, "Unauthorized: %v", err)
	}

	chanID, _ := strconv.ParseInt(authResp.ChannelId, 10, 32)
	chanID32 := int32(chanID)

	// 2. Create Session in DB
	// Logic: This marks 'is_live = true'. If this succeeds but Redis fails, we have a problem.
	session, err := s.DBClient.CreateSession(ctx, &pb_db.CreateSessionRequest{
		ChannelId:  chanID32,
		Resolution: req.Resolution,
		Bitrate:    req.Bitrate,
		Codec:      req.Codec,
	})
	if err != nil {
		if strings.Contains(err.Error(), "already live") {
			return nil, status.Error(codes.AlreadyExists, "Channel is already live")
		}
		return nil, status.Errorf(codes.Internal, "DB Error: %v", err)
	}

	// 3. Get Metadata (User/Channel)
	channel, _ := s.DBClient.GetChannel(ctx, &pb_db.GetChannelRequest{Id: &chanID32})
	username := "Streamer"
	if userResp, err := s.UserClient.GetUserByID(ctx, &pb_user.GetUserByIDRequest{UserId: channel.UserId}); err == nil {
		username = userResp.Profile.Username
	}

	// 4. Atomic Cache Write
	cacheData, _ := json.Marshal(LiveCache{
		ChannelID:   chanID32,
		SessionID:   session.Id,
		Username:    username,
		Title:       channel.Title,
		Description: channel.Description,
		Resolution:  req.Resolution,
	})

	pipe := s.Redis.Pipeline()
	pipe.Set(ctx, fmt.Sprintf("live:%d", chanID32), cacheData, 5*time.Minute) // 5m TTL for safety
	pipe.Set(ctx, fmt.Sprintf("sid_to_chan:%d", session.Id), chanID32, 5*time.Minute)

	if _, err := pipe.Exec(ctx); err != nil {
		// FAIL-SAFE: If Redis fails, tell DB to end the session immediately so the channel isn't stuck
		s.DBClient.EndSession(ctx, &pb_db.EndSessionRequest{Id: session.Id})
		return nil, status.Error(codes.Unavailable, "infrastructure sync failure (cache)")
	}

	return &pb.StartStreamResponse{SessionId: session.Id, ChannelId: chanID32}, nil
}

func (s *StreamManager) UpdateSessionMetrics(ctx context.Context, req *pb.UpdateSessionMetricsRequest) (*pb.UpdateSessionMetricsResponse, error) {

	// 1. Update Persistent DB
	// This maps to your DB Service's UpdateSessionRequest
	_, err := s.DBClient.UpdateSession(ctx, &pb_db.UpdateSessionRequest{
		Id:         req.SessionId,
		Resolution: &req.Resolution,
		Bitrate:    &req.Bitrate,
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "persistence layer update failed")
	}

	// 2. Update Discovery Cache (Redis)
	chanID, err := s.Redis.Get(ctx, fmt.Sprintf("sid_to_chan:%d", req.SessionId)).Int()
	if err == nil {
		cacheKey := fmt.Sprintf("live:%d", chanID)
		if data, err := s.Redis.Get(ctx, cacheKey).Bytes(); err == nil {
			var cached LiveCache
			json.Unmarshal(data, &cached)
			cached.Resolution = req.Resolution

			newData, _ := json.Marshal(cached)
			s.Redis.Set(ctx, cacheKey, newData, 2*time.Minute)
		}
	}

	return &pb.UpdateSessionMetricsResponse{Success: true}, nil
}

func (s *StreamManager) UpdateStreamMetadata(ctx context.Context, req *pb.UpdateMetadataRequest) (*pb.UpdateMetadataResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	authValues := md.Get("authorization")

	var targetChannelID int32

	// 1. IDENTIFICATION BRANCH
	if len(authValues) > 0 {
		// --- USER PATH: Validate JWT ---
		token := strings.TrimPrefix(authValues[0], "Bearer ")
		authResp, err := s.AuthClient.ValidateToken(ctx, &pb_auth.ValidateTokenRequest{Token: token})
		if err != nil || !authResp.IsValid {
			return nil, status.Error(codes.Unauthenticated, "invalid token")
		}

		// Lookup channel by UserID from token
		channel, err := s.DBClient.GetChannel(ctx, &pb_db.GetChannelRequest{UserId: &authResp.UserId})
		if err != nil {
			return nil, status.Errorf(codes.NotFound, "channel not found for user")
		}
		targetChannelID = channel.Id
	} else {
		// --- INTERNAL PATH: Trust via mTLS ---
		// Since you mentioned mTLS is active, if there's no token, we assume
		// it's a trusted internal service (Ingest/Transcoder).
		// Internal services MUST provide the ChannelId in the request.
		fmt.Println(req)
		if req.ChannelId == 0 {
			return nil, status.Error(codes.InvalidArgument, "channel_id required for internal updates")
		}
		targetChannelID = req.ChannelId
	}

	// 2. DATABASE UPDATE
	_, err := s.DBClient.UpdateChannel(ctx, &pb_db.UpdateChannelRequest{
		Id:          targetChannelID,
		Title:       &req.Title,
		Description: &req.Description,
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "db update failed: %v", err)
	}

	// 3. CACHE EVICTION (The Failproof Sync)
	// Instead of Get/Unmarshal/Set, we delete the key.
	// The next Heartbeat will rehydrate the cache with the fresh DB data.
	cacheKey := fmt.Sprintf("live:%d", targetChannelID)
	s.Redis.Del(ctx, cacheKey)

	log.Printf("[METADATA] Updated channel %d (Mode: %s)", targetChannelID, map[bool]string{true: "User", false: "Internal"}[len(authValues) > 0])

	return &pb.UpdateMetadataResponse{Success: true}, nil
}

func (s *StreamManager) GetLiveStreams(ctx context.Context, req *pb.GetLiveStreamsRequest) (*pb.GetLiveStreamsResponse, error) {
	var streams []*pb.StreamInfo
	var cursor uint64

	// 1. Use SCAN instead of KEYS to prevent blocking Redis
	for {
		keys, nextCursor, err := s.Redis.Scan(ctx, cursor, "live:*", 100).Result()
		if err != nil {
			log.Printf("[ERROR] Redis Scan failed: %v", err)
			break
		}

		for _, key := range keys {
			data, err := s.Redis.Get(ctx, key).Bytes()
			if err != nil {
				continue
			}

			var c LiveCache
			if err := json.Unmarshal(data, &c); err != nil {
				log.Printf("[WARN] Failed to unmarshal cache for key %s", key)
				continue
			}

			// 2. Fetch Viewer Count
			// We use a simple Get; in high-traffic, you'd MGET these
			vc, _ := s.Redis.Get(ctx, fmt.Sprintf("viewers:%d", c.SessionID)).Int()

			streams = append(streams, &pb.StreamInfo{
				ChannelId:   c.ChannelID,
				SessionId:   c.SessionID,
				Username:    c.Username,
				Title:       c.Title,
				Description: c.Description,
				ViewerCount: int32(vc),
				Resolution:  c.Resolution,
			})
		}

		cursor = nextCursor
		if cursor == 0 {
			break
		}
	}

	// 3. Fallback / Self-Healing Check
	// If the list is empty, it's possible Redis just restarted.
	// The next Heartbeats will fix this, but for this specific request,
	// we return whatever is currently in the cache.

	log.Printf("[DISCOVERY] Returning %d live streams", len(streams))
	return &pb.GetLiveStreamsResponse{Streams: streams}, nil
}

func (s *StreamManager) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	// 1. Identify the specific Distribution Node calling us
	// This is vital for horizontal scaling so we don't overwrite other nodes' counts.
	p, ok := peer.FromContext(ctx)
	nodeID := "unknown_node"
	if ok {
		nodeID = p.Addr.String()
	}

	sidKey := fmt.Sprintf("sid_to_chan:%d", req.SessionId)

	// 2. Redis/Recovery Logic (Session Mapping)
	val, err := s.Redis.Get(ctx, sidKey).Result()
	if err == redis.Nil {
		log.Printf("[HEARTBEAT] Cache miss for Session %d. Recovering...", req.SessionId)
		dbSess, err := s.DBClient.GetSession(ctx, &pb_db.GetSessionRequest{Id: req.SessionId})
		if err != nil || dbSess.Status != pb_db.StreamStatus_LIVE {
			log.Printf("[TERMINATE] Session %d not active in DB.", req.SessionId)
			return &pb.HeartbeatResponse{ShouldContinue: false}, nil
		}
		s.rehydrateCache(ctx, dbSess)
		val = fmt.Sprintf("%d", dbSess.ChannelId)
	} else if err != nil {
		return nil, status.Errorf(codes.Internal, "Redis error: %v", err)
	}

	// 3. Update Watchdog (Last Heartbeat Timestamp)
	_, hbErr := s.DBClient.UpdateSessionHeartbeat(ctx, &pb_db.UpdateSessionHeartbeatRequest{
		SessionId: req.SessionId,
	})
	if hbErr != nil {
		log.Printf("[WARN] Failed to update DB heartbeat for %d: %v", req.SessionId, hbErr)
	}

	// 4. Per-Node Viewer Tracking (The Horizontal Scaling Fix)
	// Store this node's snapshot with a short TTL (45s)
	nodeViewerKey := fmt.Sprintf("node_viewers:%d:%s", req.SessionId, nodeID)
	nodeSetKey := fmt.Sprintf("session_nodes:%d", req.SessionId)

	pipe := s.Redis.Pipeline()
	pipe.Set(ctx, nodeViewerKey, req.CurrentViewers, 45*time.Second) // Snapshot for THIS node
	pipe.SAdd(ctx, nodeSetKey, nodeID)                               // Register node in the session set
	pipe.Expire(ctx, nodeSetKey, 2*time.Minute)                      // Cleanup set if all nodes die
	_, err = pipe.Exec(ctx)
	if err != nil {
		log.Printf("[ERROR] Redis pipeline failed: %v", err)
	}

	// 5. Calculate Global Sum (Aggregation)
	totalViewers := s.calculateGlobalViewers(ctx, req.SessionId)

	// 6. Update DB with Snapshot (Not Increment)
	// Note: Change 'ViewCountIncrement' to a field that SETS the value in your DB
	chanID, _ := strconv.Atoi(val)
	statusLive := pb_db.StreamStatus_LIVE

	_, upErr := s.DBClient.UpdateProcessingStatus(ctx, &pb_db.UpdateProcessingStatusRequest{
		SessionId:        req.SessionId,
		Status:           &statusLive,
		CurrentViewCount: &totalViewers, // <--- Use a 'Set' field, not 'Increment'
	})
	if upErr != nil {
		log.Printf("[ERROR] DB status update failed: %v", upErr)
	}

	// 7. Refresh Session TTLs
	finalPipe := s.Redis.Pipeline()
	finalPipe.Expire(ctx, fmt.Sprintf("live:%d", chanID), 2*time.Minute)
	finalPipe.Expire(ctx, sidKey, 2*time.Minute)
	finalPipe.Exec(ctx)

	return &pb.HeartbeatResponse{ShouldContinue: true}, nil
}

// Helper to sum viewers from all active distribution nodes
func (s *StreamManager) calculateGlobalViewers(ctx context.Context, sessionID int64) int32 {
	nodeSetKey := fmt.Sprintf("session_nodes:%d", sessionID)
	nodes, _ := s.Redis.SMembers(ctx, nodeSetKey).Result()

	var total int32
	for _, node := range nodes {
		countKey := fmt.Sprintf("node_viewers:%d:%s", sessionID, node)
		val, err := s.Redis.Get(ctx, countKey).Result()

		if err == nil {
			count, _ := strconv.Atoi(val)
			total += int32(count)
		} else {
			// Node key expired (crashed or scaled down), remove from set
			s.Redis.SRem(ctx, nodeSetKey, node)
		}
	}
	return total
}

// Helper to re-fill Redis if it was wiped
func (s *StreamManager) rehydrateCache(ctx context.Context, sess *pb_db.SessionResponse) {
	chanID := sess.ChannelId

	// 1. Fetch Channel Metadata
	channel, err := s.DBClient.GetChannel(ctx, &pb_db.GetChannelRequest{Id: &chanID})
	if err != nil {
		log.Printf("[RECOVERY_ERR] Could not fetch channel %d for rehydration: %v", chanID, err)
		return
	}

	// 2. Fetch Username from User Service
	username := "Streamer" // Fallback default
	userResp, err := s.UserClient.GetUserByID(ctx, &pb_user.GetUserByIDRequest{UserId: channel.UserId})
	if err != nil {
		log.Printf("[RECOVERY_WARN] Could not fetch user %d, using default: %v", channel.UserId, err)
	} else {
		username = userResp.Profile.Username
	}

	// 3. Build Cache Object
	cache := LiveCache{
		ChannelID:   chanID,
		SessionID:   sess.Id,
		Username:    username,
		Title:       channel.Title,
		Description: channel.Description,
		Resolution:  sess.Resolution,
	}

	// 4. Update Redis
	data, err := json.Marshal(cache)
	if err != nil {
		log.Printf("[RECOVERY_ERR] Failed to marshal cache data: %v", err)
		return
	}

	// Use a pipeline to ensure both keys are set with the same TTL
	pipe := s.Redis.Pipeline()
	pipe.Set(ctx, fmt.Sprintf("live:%d", chanID), data, 2*time.Minute)
	pipe.Set(ctx, fmt.Sprintf("sid_to_chan:%d", sess.Id), chanID, 2*time.Minute)

	if _, err := pipe.Exec(ctx); err != nil {
		log.Printf("[RECOVERY_ERR] Failed to write to Redis: %v", err)
	} else {
		log.Printf("[RECOVERY_SUCCESS] Rehydrated cache for Session %d (User: %s)", sess.Id, username)
	}
}

func (s *StreamManager) StopStream(ctx context.Context, req *pb.StopStreamRequest) (*pb.StopStreamResponse, error) {
	log.Printf("[StopStream] Request to end Session %d", req.SessionId)

	// 1. Fetch session and CHECK STATUS
	sess, err := s.DBClient.GetSession(ctx, &pb_db.GetSessionRequest{Id: req.SessionId})
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "session not found")
	}

	// --- NEW: Status Guard ---
	// Assuming your DB uses strings or enums. If status != LIVE, it's already over.
	if sess.Status != pb_db.StreamStatus_LIVE {
		log.Printf("[WARN] StopStream called on session %d with status %s", req.SessionId, sess.Status)
		return nil, status.Errorf(codes.FailedPrecondition, "stream is already finished (Status: %s)", sess.Status)
	}

	var durationSeconds uint32
	if sess.StartTime != nil {
		durationSeconds = uint32(time.Since(sess.StartTime.AsTime()).Seconds())
	}

	// 2. Release DB Lock (Update status to COMPLETE and channels.is_live to false)
	_, err = s.DBClient.EndSession(ctx, &pb_db.EndSessionRequest{Id: req.SessionId})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to close session")
	}

	// 3. Cache Cleanup
	pipe := s.Redis.Pipeline()
	pipe.Del(ctx, fmt.Sprintf("sid_to_chan:%d", req.SessionId))
	pipe.Del(ctx, fmt.Sprintf("live:%d", sess.ChannelId))
	pipe.Exec(ctx)

	return &pb.StopStreamResponse{
		DurationSeconds: durationSeconds,
	}, nil
}

func (s *StreamManager) RunZombieWatchdog(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.cleanupZombies(ctx)
		}
	}
}

func (s *StreamManager) cleanupZombies(ctx context.Context) {
	// 1. Ask DB for IDs of dead streams
	resp, err := s.DBClient.GetStaleSessions(ctx, &pb_db.GetStaleSessionsRequest{ThresholdSeconds: 60})
	if err != nil {
		return
	}

	for _, sessionID := range resp.SessionIds {
		log.Printf("[WATCHDOG] Killing zombie session %d", sessionID)
		// 2. Reuse your StopStream logic!
		// This ensures Redis is cleared and the channel is unlocked.
		s.StopStream(ctx, &pb.StopStreamRequest{
			SessionId: sessionID,
		})
	}
}
