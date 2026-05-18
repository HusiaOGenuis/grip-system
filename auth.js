// --------------------------------------------------
// SUPABASE
// --------------------------------------------------

const SUPABASE_URL =
    "https://lxldqhgevpssgkqtosnz.supabase.co"

const SUPABASE_ANON_KEY =
    "yJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Imx4bGRxaGdldnBzc2drcXRvc256Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3Nzc1MDEwOTYsImV4cCI6MjA5MzA3NzA5Nn0.Gngx4y6VMrOBISyICnA-pcHpp3NIWq_VITKOTskg7MQ"

const supabase = window.supabase.createClient(
    SUPABASE_URL,
    SUPABASE_ANON_KEY
)

// --------------------------------------------------
// ELEMENTS
// --------------------------------------------------

const authStatus =
    document.getElementById("auth-status")

const userBox =
    document.getElementById("user-box")

// --------------------------------------------------
// LOGIN
// --------------------------------------------------

async function loginWithGoogle() {

    const { error } =
        await supabase.auth.signInWithOAuth({

            provider: "google",

            options: {
                redirectTo:
                    "https://grip-dashboard.onrender.com/app"
            }
        })

    if (error) {

        console.error(error)

        alert(error.message)
    }
}

// --------------------------------------------------
// LOGOUT
// --------------------------------------------------

async function logout() {

    await supabase.auth.signOut()

    window.location.reload()
}

// --------------------------------------------------
// SESSION
// --------------------------------------------------

async function checkSession() {

    const {
        data: { session },
        error
    } = await supabase.auth.getSession()

    if (error) {

        authStatus.innerText =
            "Session Error"

        console.error(error)

        return
    }

    if (!session) {

        authStatus.innerText =
            "NOT AUTHENTICATED"

        userBox.innerHTML = ""

        return
    }

    authStatus.innerText =
        "AUTHENTICATED"

    const user =
        session.user

    userBox.innerHTML = `
        <h3>User</h3>

        <p>
            <strong>Email:</strong>
            ${user.email}
        </p>

        <p>
            <strong>ID:</strong>
            ${user.id}
        </p>
    `
}

// --------------------------------------------------
// AUTH LISTENER
// --------------------------------------------------

supabase.auth.onAuthStateChange(
    async (event, session) => {

        console.log(
            "AUTH EVENT:",
            event
        )

        await checkSession()
    }
)

// --------------------------------------------------
// START
// --------------------------------------------------

checkSession()