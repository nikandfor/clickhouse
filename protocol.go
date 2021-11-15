package clickhouse

type (
	ClientPacket int
	ServerPacket int
)

const (
	ClientHello ClientPacket = iota
	ClientQuery
	ClientData
	ClientCancel
	ClientPing
)

const (
	ServerHello ServerPacket = iota
	ServerData
	ServerException
	ServerProgress
	ServerPong
	ServerEndOfStream
	ServerProfileInfo
	ServerTotals
	ServerExtremes
)

const (
	DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE          = 54058
	DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO = 54060
)
