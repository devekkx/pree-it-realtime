package conn

import (
	"sync"

	"github.com/google/uuid"
	"go.uber.org/zap"
)

// Manager tracks all active WebSocket clients, indexed by user ID.
type Manager struct {
	mu      sync.RWMutex
	clients map[string]*Client               // client ID - client
	users   map[uuid.UUID]map[string]*Client // user ID - client ID - client
	logger  *zap.Logger
}

func NewManager(logger *zap.Logger) *Manager {
	return &Manager{
		clients: make(map[string]*Client),
		users:   make(map[uuid.UUID]map[string]*Client),
		logger:  logger,
	}
}

func (m *Manager) Register(c *Client) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.clients[c.ID] = c

	if m.users[c.UserID] == nil {
		m.users[c.UserID] = make(map[string]*Client)
	}
	m.users[c.UserID][c.ID] = c

	m.logger.Debug("client registered",
		zap.String("client_id", c.ID),
		zap.String("user_id", c.UserID.String()),
		zap.Int("user_connections", len(m.users[c.UserID])),
	)
}

func (m *Manager) Unregister(c *Client) {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.clients, c.ID)

	if conns, ok := m.users[c.UserID]; ok {
		delete(conns, c.ID)
		if len(conns) == 0 {
			delete(m.users, c.UserID)
		}
	}

	m.logger.Debug("client unregistered",
		zap.String("client_id", c.ID),
		zap.String("user_id", c.UserID.String()),
	)
}

// GetClientsByUserID returns all active connections for a user.
func (m *Manager) GetClientsByUserID(userID uuid.UUID) []*Client {
	m.mu.RLock()
	defer m.mu.RUnlock()

	conns, ok := m.users[userID]
	if !ok {
		return nil
	}

	result := make([]*Client, 0, len(conns))
	for _, c := range conns {
		result = append(result, c)
	}
	return result
}

// IsUserOnline checks if a user has at least one active connection.
func (m *Manager) IsUserOnline(userID uuid.UUID) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	conns, ok := m.users[userID]
	return ok && len(conns) > 0
}

// OnlineUserIDs returns all user IDs with active connections.
func (m *Manager) OnlineUserIDs() []uuid.UUID {
	m.mu.RLock()
	defer m.mu.RUnlock()

	ids := make([]uuid.UUID, 0, len(m.users))
	for id := range m.users {
		ids = append(ids, id)
	}
	return ids
}

// ClientCount returns total active connections.
func (m *Manager) ClientCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.clients)
}
