import argparse

def build_parser():
    parser = argparse.ArgumentParser(description="테이블별 CSV 생성기")
    subparsers = parser.add_subparsers(dest="table", required=True)

    member = subparsers.add_parser("member", help="member 테이블")
    member.add_argument("--rows", type=int, required=True, help="생성할 멤버 수")

    block = subparsers.add_parser("block",help="block 테이블")
    block.add_argument("--rows", type=int, required=True, help="생성할 차단 개수")

    friend = subparsers.add_parser("friend",help="friend 테이블")
    friend.add_argument("--pairs",type=int,required=True,help="생성할 친구 쌍 수")

    friend_request = subparsers.add_parser("friend_request",help="friend_request 테이블")
    friend_request.add_argument("--rows",type=int,required=True,help="생성할 친구 요청 개수")

    member_champion = subparsers.add_parser("member_champion",help="member_champion 테이블")
    member_champion.add_argument("--per-member",type=int, required=True,help="한 멤버당 챔피언 개수")

    member_game_style = subparsers.add_parser("member_game_style",help="member_game_style 테이블")
    member_game_style.add_argument('--per-member',type=int,required=True,choices=range(1, 17),help="한 멤버당 게임스타일 개수")

    member_want_positions = subparsers.add_parser("member_want_positions",help="member_want_positions")
    member_want_positions.add_argument("--per-member",type=int,required=True,choices=range(1, 7),help="한 멤버당 생성할 원하는 포지션 개수")

    refresh_token = subparsers.add_parser("refresh_token", help="refresh_token 테이블")

    notification = subparsers.add_parser("notification",help="notification")
    notification.add_argument("--rows",type=int,required=True,help="생성할 알림 개수")

    matching_record = subparsers.add_parser("matching_record",help="matching_record")
    matching_record.add_argument("--per-member",type=int,required=True,help="한 멤버당 매칭 레코드 개수")
    
    manner_rating = subparsers.add_parser("manner_rating",help="manner_rating")
    manner_rating.add_argument("--per-member",type=int,required=True,help="한 멤버당 매너평가 개수")

    board = subparsers.add_parser("board",help="board")
    board.add_argument("--rows",type=int,required=True,help="생성할 게시글 개수")

    report = subparsers.add_parser("report",help="report")
    report.add_argument("--rows",type=int,required=True,help="생성할 신고 건수")

    chatroom = subparsers.add_parser("chatroom", help="chatroom 테이블")
    chatroom.add_argument("--chatrooms", type=int, required=True, help="생성할 채팅방 수")
    chatroom.add_argument("--messages-per-room", type=int, required=True, help="생성할 채팅방당 메시지 수")

    return parser