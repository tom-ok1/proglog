package discovery

import (
	"net"

	"github.com/hashicorp/serf/serf"
	"go.uber.org/zap"
)

type Membership struct {
	Config
	handler Handler
	serf    *serf.Serf
	events  chan serf.Event
	logger  *zap.Logger
}

type Config struct {
	NodeName       string
	BindAddr       string
	Tags           map[string]string
	StartJoinAddrs []string
}

type Handler interface {
	Join(name, addr string) error
	Leave(name string) error
}

func New(handler Handler, config Config) (*Membership, error) {
	m := &Membership{
		Config:  config,
		handler: handler,
		logger:  zap.L().Named("membership"),
		events:  make(chan serf.Event, 16),
	}

	addr, err := net.ResolveTCPAddr("tcp", m.BindAddr)
	if err != nil {
		return nil, err
	}

	serfConfig := serf.DefaultConfig()
	serfConfig.Init()
	serfConfig.NodeName = config.NodeName
	serfConfig.MemberlistConfig.BindAddr = addr.IP.String()
	serfConfig.MemberlistConfig.BindPort = addr.Port
	serfConfig.Tags = config.Tags
	serfConfig.EventCh = m.events

	m.serf, err = serf.Create(serfConfig)
	if err != nil {
		return nil, err
	}

	if len(config.StartJoinAddrs) > 0 {
		_, err = m.serf.Join(config.StartJoinAddrs, true)
		if err != nil {
			return nil, err
		}
	}

	go m.eventHandler()

	return m, nil
}

func (m *Membership) Members() []serf.Member {
	return m.serf.Members()
}

func (m *Membership) Leave() error {
	return m.serf.Leave()
}

func (m *Membership) eventHandler() {
	for e := range m.events {
		switch e.EventType() {
		case serf.EventMemberJoin:
			for _, member := range e.(serf.MemberEvent).Members {
				if m.isLocal(member) {
					continue
				}

				m.handler.Join(member.Name, member.Tags["rpc_addr"])
			}
		case serf.EventMemberLeave, serf.EventMemberFailed:
			for _, member := range e.(serf.MemberEvent).Members {
				if m.isLocal(member) {
					continue
				}

				m.handler.Leave(member.Name)
			}
		}
	}
}

func (m *Membership) isLocal(member serf.Member) bool {
	return m.serf.LocalMember().Name == member.Name
}

func (m *Membership) logError(err error, msg string, member serf.Member) {
	m.logger.Error(
		msg,
		zap.Error(err),
		zap.String("name", member.Name),
		zap.String("rpc_addr", member.Tags["rpc_addr"]),
	)
}
