// A QuickFIX acceptor, used as an independent judge of this project's gateway.
//
// # Why this exists
//
// The risk list for this project puts it plainly: tested only against your own
// simulator, "correct" means "consistent with my own reading of the spec". The
// 44 in-process session checks are that simulator — they can construct inputs a
// real implementation will not produce on demand, which is exactly what makes
// them useful and exactly what makes them insufficient.
//
// This binary shares **no code** with the gateway. It links QuickFIX, speaks
// FIX 4.4 over a socket, and reports what it saw. If our messages are malformed,
// missing a required field, ordered wrongly, or carry a bad checksum, QuickFIX
// rejects them and this says so.
//
// # Two constraints worth knowing about
//
// **It is compiled as C++14.** QuickFIX 1.15 uses dynamic exception
// specifications (`throw(...)`), which C++17 removed and C++20 rejects outright.
// The rest of this repository is C++20. Isolating the dependency in its own
// target with its own standard is not a workaround for a build problem — it is
// the shape that keeps the independent implementation actually independent.
//
// **It runs without a data dictionary.** Debian's `libquickfix-dev` is a
// `+dfsg` package: the FIX XML dictionaries are stripped, because FIX Protocol
// Ltd's spec files are not DFSG-free. `UseDataDictionary=N` therefore.
//
// That limits what this proves, and the limit is worth stating. QuickFIX still
// does the whole **session layer** — logon, sequence checking, resend requests,
// gap fill, heartbeats, logout — which is what milestone 7 is about. What it
// does not do is validate application message *content* against the spec. So
// this cross-check covers session semantics and framing, not whether a
// NewOrderSingle carries every field FIX 4.4 requires of one.

#include <atomic>
#include <csignal>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <map>
#include <string>

#include <quickfix/Application.h>
#include <quickfix/Fields.h>
#include <quickfix/FileLog.h>
#include <quickfix/FileStore.h>
#include <quickfix/Message.h>
#include <quickfix/Session.h>
#include <quickfix/SessionSettings.h>
#include <quickfix/SocketAcceptor.h>
#include <quickfix/fix44/Message.h>

namespace {

std::atomic<bool> stop_requested{false};

void on_signal(int) { stop_requested = true; }

/// What the acceptor observed, written out for the test script to assert on.
struct Observations {
    int logons = 0;
    int logouts = 0;
    int app_messages = 0;
    int rejects_sent = 0;
    int resend_requests_sent = 0;
    int sequence_resets_received = 0;
    int gap_fills_received = 0;
    std::map<std::string, int> admin_by_type;
    std::string last_error;
};

Observations obs;

class Judge : public FIX::Application {
  public:
    void onCreate(const FIX::SessionID&) override {}

    void onLogon(const FIX::SessionID& id) override {
        ++obs.logons;
        std::cerr << "quickfix: logged on " << id.toString() << "\n";
    }

    void onLogout(const FIX::SessionID& id) override {
        ++obs.logouts;
        std::cerr << "quickfix: logged out " << id.toString() << "\n";
    }

    void toAdmin(FIX::Message& m, const FIX::SessionID&) override {
        const std::string type = m.getHeader().getField(FIX::FIELD::MsgType);
        // What QuickFIX chooses to send back is the interesting half: a
        // ResendRequest means it saw a gap in our stream, and a Reject means it
        // disliked something we sent.
        if (type == "2") {
            ++obs.resend_requests_sent;
            std::cerr << "quickfix: sending a ResendRequest -- it saw a gap\n";
        } else if (type == "3") {
            ++obs.rejects_sent;
            std::cerr << "quickfix: REJECTING one of our messages: " << m.toString() << "\n";
        }
    }

    void toApp(FIX::Message&, const FIX::SessionID&) throw(FIX::DoNotSend) override {}

    void fromAdmin(const FIX::Message& m, const FIX::SessionID&) throw(
        FIX::FieldNotFound, FIX::IncorrectDataFormat, FIX::IncorrectTagValue,
        FIX::RejectLogon) override {
        FIX::MsgType msg_type;
        if (!m.getHeader().getFieldIfSet(msg_type)) {
            return;
        }
        const std::string type = msg_type.getValue();
        ++obs.admin_by_type[type];
        if (type == "4") {
            ++obs.sequence_resets_received;
            FIX::GapFillFlag gap_fill;
            if (m.getFieldIfSet(gap_fill) && gap_fill.getValue()) {
                ++obs.gap_fills_received;
                std::cerr << "quickfix: accepted a gap fill from the gateway\n";
            }
        }
    }

    void fromApp(const FIX::Message&, const FIX::SessionID&) throw(
        FIX::FieldNotFound, FIX::IncorrectDataFormat, FIX::IncorrectTagValue,
        FIX::UnsupportedMessageType) override {
        ++obs.app_messages;
    }
};

}  // namespace

int main(int argc, char** argv) {
    if (argc < 3) {
        std::cerr << "usage: quickfix-acceptor CONFIG SECONDS [OBSERVATIONS_PATH]\n";
        return 2;
    }
    const std::string config = argv[1];
    const int seconds = std::atoi(argv[2]);
    const std::string out_path = (argc > 3) ? argv[3] : std::string();

    std::signal(SIGINT, on_signal);
    std::signal(SIGTERM, on_signal);

    try {
        FIX::SessionSettings settings(config);
        Judge app;
        FIX::FileStoreFactory store(settings);
        FIX::FileLogFactory log(settings);
        FIX::SocketAcceptor acceptor(app, store, settings, log);

        acceptor.start();
        for (int i = 0; i < seconds * 10 && !stop_requested; ++i) {
            FIX::process_sleep(0.1);
        }
        acceptor.stop();
    } catch (const std::exception& e) {
        obs.last_error = e.what();
        std::cerr << "quickfix: " << e.what() << "\n";
    }

    // key=value, matching the shape the rest of this project's scripts assert on.
    std::ostream* out = &std::cout;
    std::ofstream file;
    if (!out_path.empty()) {
        file.open(out_path);
        out = &file;
    }
    *out << "logons=" << obs.logons << "\n"
         << "logouts=" << obs.logouts << "\n"
         << "app_messages=" << obs.app_messages << "\n"
         << "rejects_sent=" << obs.rejects_sent << "\n"
         << "resend_requests_sent=" << obs.resend_requests_sent << "\n"
         << "sequence_resets_received=" << obs.sequence_resets_received << "\n"
         << "gap_fills_received=" << obs.gap_fills_received << "\n"
         << "error=" << obs.last_error << "\n";
    for (const auto& kv : obs.admin_by_type) {
        *out << "admin_" << kv.first << "=" << kv.second << "\n";
    }
    return 0;
}
