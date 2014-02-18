// Boolean emulation
boolFalse = Bool;
boolTrue = Bool;

// Vectors
vector # [ int ] = Vector<int>;
vector # [ long ] = Vector<long>;
vector # [ User ] = Vector<User>;
vector # [ Chat ] = Vector<Chat>;
vector # [ Photo ] = Vector<Photo>;
vector # [ Message ] = Vector<Message>;
vector # [ InputUser ] = Vector<InputUser>;
vector # [ InputPeer ] = Vector<InputPeer>;
vector # [ ChatParticipant ] = Vector<ChatParticipant>;
vector # [ Contact ] = Vector<Contact>;
vector # [ ImportedPhone ] = Vector<ImportedPhone>;
vector # [ ContactRequest ] = Vector<ContactRequest>;
vector # [ ContactFound ] = Vector<ContactFound>;
vector # [ ContactLocated ] = Vector<ContactLocated>;
vector # [ SchemeType ] = Vector<SchemeType>;
vector # [ SchemeMethod ] = Vector<SchemeMethod>;
vector # [ SchemeParam ] = Vector<SchemeParam>;
vector # [ PhotoSize ] = Vector<PhotoSize>;
vector # [ Update ] = Vector<Update>;


// Error, may replace every *Result method
error code:int text:string = Error;
richError code:int type:string description:string debug:string request_params:string = Error;

// Null can replace every field in response
null = Null;

///////////////// GLOBAL INPUT /////////////////

// Input Peer (user or chat)
inputPeerEmpty = InputPeer;
inputPeerSelf = InputPeer;
inputPeerContact user_id:int = InputPeer;
inputPeerForeign user_id:int access_hash:string = InputPeer;
inputPeerChat chat_id:int = InputPeer;

// Input User
inputUserEmpty = InputUser;
inputUserSelf = InputUser;
inputUserContact user_id:int = InputUser;
inputUserForeign user_id:int access_hash:string = InputUser;

// Input Geo Point
inputGeoPointEmpty = InputGeoPoint;
inputGeoPoint lat:double long:double = InputGeoPoint;

///////////////// GLOBAL OUTPUT /////////////////

// Peer
peerUser user_id:int = Peer;
peerChat chat_id:int = Peer;

// Objects
users users:Vector<User> = Users;
chats chats:Vector<Chat> = Chats;

// User object
userEmpty id:int = User;
userSelf id:int first_name:string last_name:string phone:string photo:UserProfilePhoto status:UserStatus = User;
userContact id:int first_name:string last_name:string phone:string photo:UserProfilePhoto status:UserStatus = User;
userForeign id:int first_name:string last_name:string access_hash:string photo:UserProfilePhoto status:UserStatus = User;
userDeleted id:int first_name:string last_name:string = User;

// UserProfilePhoto object
userProfilePhotoEmpty = UserProfilePhoto;
userProfilePhoto photo_small:string photo_medium:string photo_big:string = UserProfilePhoto;

// UserStatus object
userStatusEmpty = UserStatus;
userStatusOnline status:string = UserStatus;
userStatusOffline status:string was_online:int = UserStatus;

// Chat Object
chatEmpty id:int = Chat;
chat id:int admin_id:int title:string participants:Vector<ChatParticipant> date:int = Chat;
chatParticipant user_id:int inviter_id:int date:int = ChatParticipant;

// Message object
messageEmpty = Message;
message id:int from_id:int to_id:Peer out:Bool unread:Bool message:string date:int geo:GeoPoint media:MessageMedia action:MessageAction = Message;
// Message media attachments
messageMediaEmpty = MessageMedia;
messageMediaPhoto photo:Photo = MessageMedia;
messageMediaAudio audio:Audio = MessageMedia;
// Message actions
messageActionEmpty = MessageAction;
messageActionChatEdit title:string = MessageAction;
messageActionChatAddUser user_id:int = MessageAction;
messageActionChatDeleteUser user_id:int = MessageAction;

// Photo object
photoEmpty id:long = Photo;
photo id:long user_id:int date:int caption:string geo:GeoPoint sizes:Vector<PhotoSize> = Photo;
wallPhoto id:long user_id:int date:int caption:string geo:GeoPoint unread:Bool sizes:Vector<PhotoSize> = Photo;
photoSizeEmpty type:string = PhotoSize;
photoSize type:string src:string w:int h:int = PhotoSize;

// Audio object
audioEmpty id:long = Audio;
audio id:long user_id:int date:int caption:string sizes:AudioFiles = Audio;
audioFiles d:AudioFile = AudioFiles;
audioFileEmpty = AudioFile;
audioFile src:string = AudioFile;

// GeoPoint object
geoPointEmpty = GeoPoint;
geoPoint long:double lat:double = GeoPoint;


///////////////// OUTPUT /////////////////

// auth
auth.checkedPhone phone_registered:Bool = auth.CheckedPhone;
auth.sentCodePreview phone_registered:Bool phone_code_hash:string phone_code_test:string = auth.SentCode;
auth.sentCode phone_registered:Bool phone_code_hash:string = auth.SentCode;
auth.authorization expires:int user:User = auth.Authorization;

// contacts
contact user_id:int mutual:Bool = Contact;
importedPhone user_id:int phone:string = ImportedPhone;
contactRequest user_id:int date:int = ContactRequest;
contactFound user_id:int = ContactFound;
contactLocated user_id:int location:GeoPoint date:int distance:int = ContactLocated;

contacts.contacts contacts:Vector<Contact> users:Vector<User> = contacts.Contacts;
contacts.importedPhones imported:Vector<ImportedPhone> users:Vector<User> = contacts.ImportedPhones;
contacts.requests requests:Vector<ContactRequest> users:Vector<User> = contacts.Requests;
contacts.requestsSlice count:int requests:Vector<ContactRequest> users:Vector<User> = contacts.Requests;
contacts.found results:Vector<ContactFound> users:Vector<User> = contacts.Found;
contacts.located results:Vector<ContactLocated> users:Vector<User> = contacts.Located;

// messages
messages.messages messages:Vector<Message> chats:Vector<Chat> users:Vector<User> = messages.Messages;
messages.messagesSlice count:int messages:Vector<Message> chats:Vector<Chat> users:Vector<User> = messages.Messages;
messages.message message:Message chats:Vector<Chat> users:Vector<User> = messages.Message;

messages.chat chat:Chat users:Vector<User> = messages.Chat;
messages.chats chats:Vector<Chat> users:Vector<User> = messages.Chats;

messages.historyState count:int messages:HistoryMessages max_read_in_id:int max_read_out_id:int wall_count:int wall_unread_count:int = messages.HistoryState;
historyNotModified = HistoryMessages;
historyMessages messages:Vector<int> = HistoryMessages;

// updates
updateNewMessage message:messages.Message pts:int = Update;
updateReadMessages messages:Vector<int> pts:int = Update;
updateDeleteMessages messages:Vector<int> pts:int = Update;
updateRestoreMessages messages:Vector<int> pts:int = Update;
updateUserTyping user_id:int = Update;
updateChatUserTyping chat_id:int user_id:int = Update;
updateUserStatus user_id:int status:UserStatus = Update;
updateUserPhoto user_id:int photo:UserProfilePhoto = Update;
updateNewPhoto user_id:int photo:photos.Photo = Update;

// updates methods
updates.state pts:int date:int = updates.State;
updates.difference new_photos:Vector<Photo> new_messages:Vector<Message> updated_contacts:Vector<int> contacts_online:Vector<int> other_updates:Vector<Update> chats:Vector<Chat> users:Vector<User> state:updates.State = updates.Difference;
updates.differenceSlice new_photos:Vector<Photo> new_messages:Vector<Message> updated_contacts:Vector<int> other_updates:Vector<Update> chats:Vector<Chat> users:Vector<User> intermediate_state:updates.State = updates.Difference;

// updates long-poll
updatesPersonal updates:Vector<Update> date:int seq:int = Updates;
updates updates:Vector<Update> date:int = Updates;

// photos
photos.photos photos:Vector<Photo> users:Vector<User> = photos.Photos;
photos.photosSlice count:int photos:Vector<Photo> users:Vector<User> = photos.Photos;
photos.photo photo:Photo users:Vector<User> = photos.Photo;


// help
scheme scheme_raw:string types:Vector<SchemeType> methods:Vector<SchemeMethod> version:int = Scheme;
schemeNotModified = Scheme;
schemeType id:int predicat:string params:Vector<SchemeParam> type:string = SchemeType;
schemeMethod id:int method:string params:Vector<SchemeParam> type:string = SchemeMethod;
schemeParam name:string type:string = SchemeParam;

// config
config date:int = Config;

// updates

---functions---

// auth
// check phone number availability
auth.checkPhone phone_number:string = auth.CheckedPhone;
// send SMS confirmation code
auth.sendCode phone_number:string = auth.SentCode;
// sign in / sign up
auth.signUp phone_number:string phone_code_hash:string phone_code:int api_id:int api_hash:string first_name:string last_name:string = auth.Authorization;
auth.signIn phone_number:string phone_code_hash:string phone_code:int api_id:int api_hash:string = auth.Authorization;
auth.logOut = Bool;

// acount
account.registerDevice token_type:int token:string device_model:string system_version:string app_version:string app_sandbox:Bool = Bool;
account.unregisterDevice token_type:int token:string = Bool;
account.updateProfile first_name:string last_name:string = User;
account.updateStatus offline:Bool status:string = Bool;

// users
users.getUsers id:Vector<InputUser> = Vector<User>;

// contacts
contacts.getContactIDs = Vector<int>;
contacts.getOnlines = Vector<int>;
contacts.getContacts = contacts.Contacts;
contacts.getRequests offset:int limit:int = contacts.Requests;
contacts.importPhones contacts:string = contacts.ImportedPhones;
contacts.search q:string limit:int = contacts.Found;
contacts.locate geo_point:InputGeoPoint radius:int limit:int = contacts.Located;
contacts.sendRequest id:InputUser = Bool;
contacts.acceptRequest id:InputUser = Bool;
contacts.declineRequest id:InputUser = Bool;

// messages
messages.getMessages id:Vector<int> = messages.Messages;
messages.getDialogs offset:int max_id:int limit:int = messages.Messages;
messages.getHistory peer:InputPeer offset:int max_id:int limit:int = messages.Messages;
messages.search peer:InputPeer q:string min_date:int max_date:int offset:int max_id:int limit:int = messages.Messages;
messages.readHistory peer:InputPeer max_id:int = Vector<int>;
messages.deleteHistory peer:InputPeer = Vector<int>;
messages.deleteMessages id:Vector<int> = Vector<int>;
messages.restoreMessages id:Vector<int> = Vector<int>;
messages.setTyping peer:InputPeer typing:Bool = Bool;
messages.getHistoryState peer:InputPeer limit:int hash:string = messages.HistoryState;
messages.sendMessage peer:InputPeer message:string geo_point:InputGeoPoint = messages.Message;

// chats
messages.getChats id:Vector<int> = messages.Chats;
messages.editChat chat_id:int title:string = messages.Message;
messages.addChatUser chat_id:int user_id:InputUser = messages.Message;
messages.deleteChatUser chat_id:int user_id:InputUser = messages.Message;
messages.createChat users:Vector<InputUser> title:string = messages.Chat;

// updates
updates.getState = updates.State;
updates.getDifference pts:int date:int = updates.Difference;

// photos
photos.getPhotos id:Vector<long> = photos.Photos;
photos.getWall user_id:InputUser offset:int max_id:int limit:int = photos.Photos;
photos.readWall user_id:InputUser max_id:long = Bool;
photos.editPhoto id:long caption:string geo_point:InputGeoPoint = photos.Photo;
photos.updateProfilePhoto id:long crop_left:int crop_top:int crop_width:int = UserProfilePhoto;
photos.deletePhotos id:Vector<long> = Vector<long>;
photos.restorePhotos id:Vector<long> = Vector<long>;

// misc
help.getConfig = Config;
help.getScheme version:int = Scheme;
